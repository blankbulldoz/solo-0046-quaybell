"""复原两种“看板相同、领取为空”的积压现场，并留下可复核的只读取证材料。

场景 a：消费组仍暂停；目标顺序键的队头租约已过期但尚未回收，目标后继被它挡住。
场景 b：消费组已恢复；目标队头已被回收，但正处在指数退避中，后继同样被挡住。

这两个现场在看板队列样本的前二十行里完全一致，目标键都不在样本中。
本模块只用公开 HTTP 入口和受控时钟：

* 先复制一份只读副本，在副本上做 GET 并保存看板、消息轨迹和证据导出，
  同时核对副本行级摘要不变、原库文件字节不变；
* 再对另一些 SQLite 副本做领取、暂停、回收和确认实验，验证解除暂停或等待是否足够；
* 原始库在整个过程中不被领取、暂停、回收或确认，也不被写事务触碰。

运行 ``python3 -m examples.triage`` 查看摘要，``--export DIR`` 写出取证包，
``--verify DIR`` 重新构造现场并与已保存的取证包逐项比对。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from quaybell.codec import fingerprint, pack
from quaybell.persistence import RelayPersistence
from quaybell.server import RelayServer

FORMAT = 'quaybell-r049-triage/1'
T0 = 1_730_000_000.0
OBSERVED = T0 + 5.0
READY = T0 + 7.0

CHANNEL = 'triage-r049'
GROUP = 'backlog-rotor'
CH = f'/v1/channels/{CHANNEL}'
GP = f'{CH}/groups/{GROUP}'
TARGET_LANE = 'target-lane'
FILLER_LANES = 10
SAMPLE_CAP = 20

# 控制车道始终占用租约 300 秒，将前十条车道的队头钉在队列前部；
# 目标队头使用 4 秒租约，在 OBSERVED 时已经过期。
FILLER_POLICY = dict(lease_seconds=300, retry_seconds=3, max_attempts=2, retention_seconds=3600)
TARGET_POLICY = dict(lease_seconds=4, retry_seconds=3, max_attempts=2, retention_seconds=3600)

SCENES = {
    'a': '组仍暂停；目标队头租约已过期但未回收',
    'b': '组已恢复；目标队头已回收，正处在退避中',
}
TABLES = ('channels', 'policies', 'subscriptions', 'messages', 'deliveries', 'attempts', 'journal')
PUBLIC_ATTEMPT = ('number', 'worker', 'started', 'expires', 'outcome', 'finished', 'reason')


class Client:
    def __init__(self, server, clock):
        self.base = f'http://127.0.0.1:{server.server_port}'
        self.clock = clock

    def call(self, path, payload=None, expected=200):
        data = None if payload is None else json.dumps(payload).encode('utf-8')
        headers = {} if data is None else {'Content-Type': 'application/json'}
        request = Request(self.base + path, data=data, headers=headers)
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            body = response.read()
            if response.status != expected:
                raise RuntimeError((path, response.status, expected, body[:400]))
            return body

    def json(self, path, payload=None, expected=200):
        return json.loads(self.call(path, payload, expected))


@contextmanager
def serving(path, now):
    clock = [float(now)]
    server = RelayServer(('127.0.0.1', 0), RelayPersistence(path), lambda: clock[0])
    thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02}, daemon=True)
    thread.start()
    try:
        yield Client(server, clock)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def envelope(producer, key, lane, value):
    return dict(producer=producer, key=key, lane=lane, payload={'value': value}, tags={})


def expect(condition, message):
    if not condition:
        raise RuntimeError(message)


def checkpoint(path):
    with sqlite3.connect(path) as db:
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')


def copy_database(source, destination):
    origin = sqlite3.connect(source)
    target = sqlite3.connect(destination)
    try:
        origin.backup(target)
    finally:
        target.close()
        origin.close()


def file_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def database_rows(path):
    """按 rowid 读取固定表的全部行，去掉租约凭据摘要。"""
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    rows = []
    try:
        for table in TABLES:
            cursor = db.execute(f'SELECT * FROM {table} ORDER BY rowid')
            columns = [item[0] for item in cursor.description]
            for row in cursor:
                item = {column: row[column] for column in columns if column != 'token_digest'}
                rows.append([table, item])
    finally:
        db.close()
    return rows


def row_digest(path):
    return fingerprint(pack(database_rows(path)))


def build_scene(scene, path):
    """用公开入口构造现场；返回目标键的标识。"""
    expect(scene in SCENES, f'未知场景 {scene}')
    with serving(path, T0) as client:
        client.json('/v1/channels', {'name': CHANNEL, 'policy': FILLER_POLICY}, 201)
        client.json(CH + '/groups', {'name': GROUP, 'tags': {}}, 201)
        for number in range(1, FILLER_LANES + 1):
            lane = f'filler-{number:02d}'
            client.json(CH + '/messages', envelope('filler-source', f'filler-{number:02d}-head',
                                                   lane, number * 2), 201)
            client.json(CH + '/messages', envelope('filler-source', f'filler-{number:02d}-next',
                                                   lane, number * 2 + 1), 201)
        filler = client.json(GP + '/claim', {'worker': 'filler-worker', 'limit': FILLER_LANES})
        expected_filler = list(range(1, FILLER_LANES * 2, 2))
        expect([item['delivery_id'] for item in filler['items']] == expected_filler,
               ('控制车道领取异常', [item['delivery_id'] for item in filler['items']]))
        client.json(CH + '/policy', TARGET_POLICY | {'expected': 1}, 201)
        head = client.json(CH + '/messages', envelope('pulse-source', 'target-head', TARGET_LANE, 100), 201)
        successor = client.json(CH + '/messages', envelope('pulse-source', 'target-next',
                                                           TARGET_LANE, 101), 201)
        target = client.json(GP + '/claim', {'worker': 'target-worker', 'limit': 1})
        expect([item['delivery_id'] for item in target['items']] == [head['fanout'][GROUP]],
               ('目标队头领取异常', [item['delivery_id'] for item in target['items']]))
        client.clock[0] = OBSERVED
        if scene == 'a':
            client.json(GP + '/pause', {'paused': True})
        else:
            client.json('/v1/sweep', {'limit': 100})
        return {
            'scene': scene,
            'label': SCENES[scene],
            'message_ids': {'head': head['message_id'], 'successor': successor['message_id']},
            'delivery_ids': {'head': head['fanout'][GROUP], 'successor': successor['fanout'][GROUP]},
            'filler_delivery_ids': [item['delivery_id'] for item in filler['items']],
        }


def check_evidence(raw):
    data = json.loads(raw)
    digest = data.pop('sha256')
    expect(fingerprint(pack(data)) == digest, '证据导出摘要不匹配')
    expect(data['format'] == 'quaybell-trace/1', '证据导出格式不符')
    return data


def sanitize_claim(reply):
    """领取回执去掉租约凭据，只保留可公开核对的事实。"""
    return {
        'paused': reply['paused'],
        'items': [{'delivery_id': item['delivery_id'], 'message_id': item['message_id'],
                   'lane': item['lane'], 'policy': item['policy'],
                   'attempt': item['lease']['number'], 'expires': item['lease']['expires']}
                  for item in reply['items']],
    }


def sanitize_outcome(item):
    return {key: item[key] for key in ('delivery_id', 'attempt', 'outcome', 'state', 'effective_at', 'reason')}


def delivery_facts(trace):
    expect(len(trace['deliveries']) == 1, ('投递数异常', len(trace['deliveries'])))
    item = trace['deliveries'][0]
    return {
        'delivery_id': item['id'],
        'message_id': item['message_id'],
        'lane': item['lane'],
        'policy': item['policy'],
        'state': item['state'],
        'ready_at': item['ready_at'],
        'terminal_at': item['terminal_at'],
        'parent_id': item['parent_id'],
        'attempts': [{key: attempt[key] for key in PUBLIC_ATTEMPT} for attempt in item['attempts']],
        'events': [{'kind': event['kind'], 'at': event['at'], 'details': event['details']}
                   for event in trace['events']],
    }


def capture_scene(path, meta):
    """先复制再读取：副本上只发 GET；核对行摘要不变，且原库文件字节不变。"""
    probe_path = Path(path).with_suffix('.reading-copy.sqlite3')
    source_before = file_digest(path)
    copy_database(path, probe_path)
    checkpoint(probe_path)
    before = row_digest(probe_path)
    with serving(probe_path, OBSERVED) as client:
        board = client.json(CH)
        head_trace = client.json(f"/v1/messages/{meta['message_ids']['head']}")
        successor_trace = client.json(f"/v1/messages/{meta['message_ids']['successor']}")
        head_raw = client.call(f"/v1/messages/{meta['message_ids']['head']}/evidence")
        successor_raw = client.call(f"/v1/messages/{meta['message_ids']['successor']}/evidence")
    after = row_digest(probe_path)
    source_after = file_digest(path)
    expect(before == after, ('只读取证改动了副本数据库行', meta['scene']))
    expect(source_before == source_after, ('只读取证改动了原库文件', meta['scene']))
    expect(after == row_digest(path), '读取副本与原库不一致')
    head_evidence = check_evidence(head_raw)
    successor_evidence = check_evidence(successor_raw)
    expect(head_evidence['observed_at'] == OBSERVED, '证据观察时间不符')
    expect(head_evidence['trace'] == head_trace, '证据与轨迹快照不一致')
    expect(successor_evidence['trace'] == successor_trace, '证据与轨迹快照不一致')
    group = next(item for item in board['groups'] if item['name'] == GROUP)
    return {
        'label': meta['label'],
        'message_ids': meta['message_ids'],
        'delivery_ids': meta['delivery_ids'],
        'board': board,
        'group': group,
        'head_trace': head_trace,
        'successor_trace': successor_trace,
        'head_facts': delivery_facts(head_trace),
        'successor_facts': delivery_facts(successor_trace),
        'head_evidence': head_evidence,
        'successor_evidence': successor_evidence,
        'head_evidence_raw': head_raw.decode('ascii'),
        'successor_evidence_raw': successor_raw.decode('ascii'),
        'database': {'digest': before, 'rows': database_rows(probe_path)},
        'read_only': {'digest_before': before, 'digest_after': after,
                      'unchanged': before == after,
                      'source_unchanged_bytewise': source_before == source_after},
    }


class Probe:
    """对一个数据库副本执行操作，并记录每一步是否改动了持久行。"""

    def __init__(self, client, meta, path, name):
        self.client = client
        self.meta = meta
        self.path = path
        self.record = {'id': name, 'source': meta['scene'], 'copy': path.name,
                       'label': meta['label'], 'steps': [], 'observations': [],
                       'findings': {}}
        self.last = row_digest(path)
        self.record['database_digest_before'] = self.last

    def set_time(self, when):
        self.client.clock[0] = float(when)

    def _record(self, entry):
        now = row_digest(self.path)
        entry['database_digest'] = now
        entry['changed_database'] = now != self.last
        self.last = now
        self.record['steps'].append(entry)
        self.last_step = entry
        return entry

    def claim(self, worker):
        raw = self.client.json(GP + '/claim', {'worker': worker, 'limit': 1})
        self._record({'at': self.client.clock[0], 'action': 'claim', 'worker': worker,
                      'reply': sanitize_claim(raw), 'token_kept_out_of_record': True})
        return raw, self.last_step

    def pause(self, paused):
        raw = self.client.json(GP + '/pause', {'paused': paused})
        self._record({'at': self.client.clock[0], 'action': 'pause', 'paused': raw['paused']})
        return raw, self.last_step

    def sweep(self, limit=100):
        raw = self.client.json('/v1/sweep', {'limit': limit})
        self._record({'at': self.client.clock[0], 'action': 'sweep',
                      'recovered': [sanitize_outcome(item) for item in raw['recovered']]})
        return raw, self.last_step

    def ack(self, delivery_id, token):
        raw = self.client.json(f'/v1/deliveries/{delivery_id}/ack', {'token': token})
        self._record({'at': self.client.clock[0], 'action': 'ack', 'delivery_id': delivery_id,
                      'state': raw['state'], 'attempt': raw['attempt'],
                      'effective_at': raw['effective_at']})
        return raw, self.last_step

    def observe(self, note):
        facts = {}
        for name, message_id in self.meta['message_ids'].items():
            facts[name] = delivery_facts(self.client.json(f'/v1/messages/{message_id}'))
        entry = self._record({'at': self.client.clock[0], 'action': 'observe',
                              'note': note, 'deliveries': facts})
        self.record['observations'].append(entry)
        return entry['deliveries']

    def finding(self, key, value):
        self.record['findings'][key] = value

    def finish(self):
        self.record['database_digest_after'] = self.last
        return self.record


def flow_a_paused_wait(probe, meta):
    probe.set_time(OBSERVED)
    first, _ = probe.claim('client-one')
    second, _ = probe.claim('client-two')
    facts = probe.observe('暂停中两个客户端依次领取')
    probe.finding('both_clients_empty_at_observe',
                  first['items'] == [] and second['items'] == []
                  and first['paused'] is True and second['paused'] is True)
    probe.finding('head_still_leased_and_expired',
                  facts['head']['state'] == 'leased'
                  and facts['head']['attempts'][0]['outcome'] == 'open'
                  and facts['head']['attempts'][0]['expires'] <= OBSERVED)
    probe.set_time(READY)
    third, _ = probe.claim('client-one')
    probe.observe('暂停中等到原截止加退避')
    probe.finding('paused_claim_empty_at_derived_ready', third['items'] == [] and third['paused'] is True)
    probe.set_time(READY + 300)
    fourth, _ = probe.claim('client-two')
    facts = probe.observe('暂停中继续等待')
    probe.finding('paused_claim_empty_after_long_wait', fourth['items'] == [] and fourth['paused'] is True)
    probe.finding('head_still_blocking_after_long_wait',
                  facts['head']['state'] == 'leased' and facts['successor']['state'] == 'waiting')
    probe.finding('database_untouched', probe.last == probe.record['database_digest_before'])


def flow_a_unpause_at_observe(probe, meta):
    probe.set_time(OBSERVED)
    probe.pause(False)
    first, first_step = probe.claim('unpause-probe')
    facts = probe.observe('解除暂停后立即观察')
    probe.finding('unpause_then_claim_empty', first['items'] == [])
    probe.finding('claim_reaped_head_into_backoff',
                  facts['head']['state'] == 'waiting' and facts['head']['ready_at'] == READY
                  and facts['head']['attempts'][0]['outcome'] == 'expired')
    probe.finding('claim_changed_database', first_step['changed_database'] is True)
    probe.set_time(OBSERVED + 1)
    during, _ = probe.claim('unpause-probe')
    probe.finding('claim_empty_one_second_into_backoff', during['items'] == [])
    probe.set_time(READY)
    ready, _ = probe.claim('unpause-probe')
    probe.finding('claim_at_derived_ready_delivers_head',
                  [item['delivery_id'] for item in ready['items']] == [meta['delivery_ids']['head']]
                  and ready['items'][0]['lease']['number'] == 2)
    probe.ack(meta['delivery_ids']['head'], ready['items'][0]['lease']['token'])
    successor, _ = probe.claim('unpause-probe')
    probe.finding('ack_unblocks_successor',
                  [item['delivery_id'] for item in successor['items']]
                  == [meta['delivery_ids']['successor']]
                  and successor['items'][0]['lease']['number'] == 1)
    facts = probe.observe('队头确认后的后继')
    probe.finding('successor_waiting_is_backoff_not_redrive',
                  facts['successor']['state'] == 'leased'
                  and facts['successor']['parent_id'] is None)


def flow_a_unpause_at_ready(probe, meta):
    probe.set_time(READY)
    probe.pause(False)
    ready, _ = probe.claim('late-unpause-probe')
    probe.finding('unpause_at_derived_ready_delivers_in_one_claim',
                  [item['delivery_id'] for item in ready['items']] == [meta['delivery_ids']['head']]
                  and ready['items'][0]['lease']['number'] == 2)
    probe.finding('no_explicit_sweep_needed',
                  all(step['action'] != 'sweep' for step in probe.record['steps']))
    probe.ack(meta['delivery_ids']['head'], ready['items'][0]['lease']['token'])
    successor, _ = probe.claim('late-unpause-probe')
    probe.finding('ack_unblocks_successor',
                  [item['delivery_id'] for item in successor['items']]
                  == [meta['delivery_ids']['successor']])
    probe.observe('晚解除暂停后的后继')


def flow_a_sweep_while_paused(probe, meta):
    probe.set_time(OBSERVED)
    recovered, _ = probe.sweep()
    facts = probe.observe('暂停中回收过期租约后')
    probe.finding('sweep_recovered_expired_head',
                  len(recovered['recovered']) == 1
                  and recovered['recovered'][0]['delivery_id'] == meta['delivery_ids']['head']
                  and recovered['recovered'][0]['effective_at'] == READY)
    probe.finding('sweep_sets_backoff_ready_at',
                  facts['head']['state'] == 'waiting' and facts['head']['ready_at'] == READY)
    probe.set_time(READY)
    paused, _ = probe.claim('paused-probe')
    facts = probe.observe('已回收但仍在暂停')
    probe.finding('claim_still_paused_empty_at_ready', paused['items'] == [] and paused['paused'] is True)
    probe.finding('sweep_does_not_bypass_pause',
                  facts['head']['state'] == 'waiting' and facts['successor']['state'] == 'waiting')
    probe.finding('claims_do_not_change_database_while_paused',
                  all(step['changed_database'] is False
                      for step in probe.record['steps'] if step['action'] == 'claim'))


def flow_b_wait_only(probe, meta):
    probe.set_time(OBSERVED)
    first, first_step = probe.claim('client-one')
    second, second_step = probe.claim('client-two')
    facts = probe.observe('已恢复组中两个客户端依次领取')
    probe.finding('both_clients_empty_at_observe',
                  first['items'] == [] and second['items'] == []
                  and first['paused'] is False and second['paused'] is False)
    probe.finding('claims_did_not_change_database',
                  first_step['changed_database'] is False and second_step['changed_database'] is False)
    probe.finding('head_already_reclaimed_and_backing_off',
                  facts['head']['state'] == 'waiting' and facts['head']['ready_at'] == READY
                  and facts['head']['attempts'][0]['outcome'] == 'expired'
                  and facts['head']['attempts'][0]['finished'] == OBSERVED)
    probe.set_time(OBSERVED + 1)
    during, _ = probe.claim('client-one')
    probe.finding('claim_empty_one_second_into_backoff', during['items'] == [])
    probe.set_time(READY)
    ready, _ = probe.claim('wait-probe')
    facts = probe.observe('退避结束')
    probe.finding('waiting_alone_delivers_head',
                  [item['delivery_id'] for item in ready['items']] == [meta['delivery_ids']['head']]
                  and ready['items'][0]['lease']['number'] == 2)
    probe.ack(meta['delivery_ids']['head'], ready['items'][0]['lease']['token'])
    successor, _ = probe.claim('wait-probe')
    probe.finding('ack_unblocks_successor',
                  [item['delivery_id'] for item in successor['items']]
                  == [meta['delivery_ids']['successor']])
    probe.finding('no_pause_or_sweep_invoked',
                  all(step['action'] not in ('pause', 'sweep') for step in probe.record['steps']))
    probe.observe('后继被领取')


EXPERIMENTS = (
    ('a-paused-wait-only', 'a', flow_a_paused_wait,
     '仍暂停，只等待：到原截止加退避也不会有投递'),
    ('a-unpause-at-observe', 'a', flow_a_unpause_at_observe,
     '在观察时解除暂停：领取会回收过期队头并转入退避'),
    ('a-unpause-at-ready', 'a', flow_a_unpause_at_ready,
     '等到推导出的就绪时刻再解除暂停：一次领取完成回收并投递'),
    ('a-sweep-while-paused', 'a', flow_a_sweep_while_paused,
     '暂停中先回收：队头转入退避，但暂停仍然挡住领取'),
    ('b-wait-only', 'b', flow_b_wait_only,
     '已恢复，只等待：退避结束即可领取，无需暂停或回收操作'),
)

SAMPLE_ONLY_CAVEATS = [
    '样本只有二十行且不包含疑似的积压目标时，不能断定目标“藏在某条被阻挡的后继里”还是“根本不存在”，'
    '也不能断定它属于哪条顺序键。',
    '样本里看不到队头尝试结果，因此不能区分“租约过期未回收”和“已回收正在退避”；这两种现场的样本完全相同。',
    '样本里看不到目标的 ready_at 和尝试次数，因此不能算出还要等多久，也不能断言再等一次就够。',
    '样本只显示当前暂停标志，不能证明暂停是空领取的唯一原因：即使解除暂停，过期未回收的队头仍要再等一个退避周期。',
    '样本没有完整计数的明细，不能据样本排除其他顺序键上的独立阻挡或迟延。',
    '仅凭样本无法判断补齐证据需要哪几个读接口，更不能据此决定是否该做回收或重投。',
    '上述复现是受控时钟下的最小现场；现场的总量、键名和退避长度都要重新读取，不能把本包里的数字当成现场读数。',
]

LIMITS = [
    '样本只包含按投递号排序的前二十条未处置投递；完整分类计数仍覆盖全部投递。',
    '暂停组的领取在回收之前返回，因此暂停期间的领取不会回收过期租约。',
    '恢复后的领取会先回收本组最多一百条到期尝试，所以领取不是中性观测。',
    '目标投递第二次尝试失败后会按 max_attempts 进入死信，本取证未覆盖该分支。',
    '本取证固定使用受控时钟和本机回环实例；现场时钟偏差或并发写入不在覆盖范围内。',
]

PROCEDURE = [
    '不改动原始库：GET /v1/channels/{channel} 读取看板与完整计数。',
    'GET /v1/messages/{id} 读取疑似目标队头与后继的投递、尝试和事件；先定位目标，再据此算就绪时刻。',
    'GET /v1/messages/{id}/evidence 下载去掉凭据的证据导出并复核 sha256。',
    '复制 SQLite 库，在副本上验证解除暂停、等待和回收，避免领取副作用落到现场。',
    '本模块 --verify 会重建两个现场，并对取证包和四份证据导出逐项比对。',
]


def run_experiment(workdir, meta, name, source, flow, label):
    path = workdir / f'{name}.sqlite3'
    copy_database(workdir / f'scene-{source}.sqlite3', path)
    with serving(path, OBSERVED) as client:
        probe = Probe(client, meta, path, name)
        flow(probe, meta)
        probe.record['head_final'] = delivery_facts(
            client.json(f"/v1/messages/{meta['message_ids']['head']}"))
        probe.record['successor_final'] = delivery_facts(
            client.json(f"/v1/messages/{meta['message_ids']['successor']}"))
    probe.record['label'] = label
    return probe.finish()


def distinguishers(scenes):
    a, b = scenes['a'], scenes['b']
    sample_a = a['group']['sample']
    sample_b = b['group']['sample']
    target_ids = {a['delivery_ids']['head'], a['delivery_ids']['successor']}
    sample_ids = {item['id'] for item in sample_a}
    counts_diff = {key: {'a': a['group']['counts'].get(key), 'b': b['group']['counts'].get(key)}
                   for key in sorted(set(a['group']['counts']) | set(b['group']['counts']))
                   if a['group']['counts'].get(key) != b['group']['counts'].get(key)}
    return {
        'sample_capped_at': SAMPLE_CAP,
        'sample_ids': [item['id'] for item in sample_a],
        'sample_length': len(sample_a),
        'sample_identical': pack(sample_a) == pack(sample_b),
        'target_delivery_ids': sorted(target_ids),
        'target_ids_in_sample': sorted(target_ids & sample_ids),
        'counts_differ': counts_diff,
        'paused': {'a': a['group']['paused'], 'b': b['group']['paused']},
        'head_state': {'a': a['head_facts']['state'], 'b': b['head_facts']['state']},
        'head_attempt_outcome': {
            'a': a['head_facts']['attempts'][0]['outcome'],
            'b': b['head_facts']['attempts'][0]['outcome'],
        },
        'head_ready_at': {'a': a['head_facts']['ready_at'], 'b': b['head_facts']['ready_at']},
        'head_event_kinds': {
            'a': [event['kind'] for event in a['head_facts']['events']],
            'b': [event['kind'] for event in b['head_facts']['events']],
        },
        'successor_facts_identical': pack(a['successor_facts']) == pack(b['successor_facts']),
        'target_keys_visible_in_board_messages': [
            item['dedup_key'] for item in a['board']['messages']
            if item['dedup_key'] in ('target-head', 'target-next')
        ],
        'target_locator_in_board_messages': [
            {'id': item['id'], 'key': item['dedup_key'], 'lane': item['lane'], 'policy': item['policy']}
            for item in a['board']['messages']
            if item['dedup_key'] in ('target-head', 'target-next')
        ],
        'database_digests': {'a': a['database']['digest'], 'b': b['database']['digest']},
    }


def build_bundle(workdir):
    meta = {}
    for scene in ('a', 'b'):
        path = workdir / f'scene-{scene}.sqlite3'
        meta[scene] = build_scene(scene, path)
        checkpoint(path)
    scenes = {scene: capture_scene(workdir / f'scene-{scene}.sqlite3', meta[scene])
              for scene in ('a', 'b')}
    experiments = [run_experiment(workdir, meta[source], name, source, flow, label)
                   for name, source, flow, label in EXPERIMENTS]
    bundle = {
        'format': FORMAT,
        'subject': '消费组积压：看板样本相同而领取为空的两种现场',
        'clock': {'t0': T0, 'observed_at': OBSERVED, 'derived_ready_at': READY,
                  'derivation': '队头租约截止 1730000004 + 策略退避 min(3 * 2**0, 3600) = 1730000007'},
        'target': {'lane': TARGET_LANE, 'head_key': 'target-head', 'successor_key': 'target-next',
                   'head_delivery_id': meta['a']['delivery_ids']['head'],
                   'successor_delivery_id': meta['a']['delivery_ids']['successor']},
        'policies': {'filler': FILLER_POLICY, 'target': TARGET_POLICY},
        'scenes': scenes,
        'distinguishers': distinguishers(scenes),
        'experiments': experiments,
        'sample_only_caveats': SAMPLE_ONLY_CAVEATS,
        'limits': LIMITS,
        'procedure': PROCEDURE,
    }
    bundle['sha256'] = fingerprint(pack(bundle))
    return bundle


def validate(bundle):
    """每一条结论都必须为真，否则本次复原不能作为取证材料。"""
    scenes, findings = bundle['scenes'], bundle['distinguishers']
    expect(findings['sample_identical'], '两个场景的看板样本不一致')
    expect(findings['sample_length'] == SAMPLE_CAP, '样本行数不是预期上限')
    expect(findings['target_ids_in_sample'] == [], '目标投递出现在样本中')
    expect(findings['successor_facts_identical'], '后继事实在两种现场不一致')
    expect(findings['paused'] == {'a': True, 'b': False}, '暂停标志不符')
    expect(findings['head_state'] == {'a': 'leased', 'b': 'waiting'}, '队头状态不符')
    expect(findings['head_attempt_outcome'] == {'a': 'open', 'b': 'expired'}, '队头尝试结果不符')
    expect(findings['head_ready_at'] == {'a': T0, 'b': READY}, '队头就绪时刻不符')
    expect(findings['counts_differ'] == {'delayed': {'a': None, 'b': 1},
                                         'expired': {'a': 1, 'b': None}}, '计数差异不符')
    expect(sorted(item['key'] for item in findings['target_locator_in_board_messages'])
           == ['target-head', 'target-next'], '看板近期消息里定位不到目标键')
    for scene in ('a', 'b'):
        expect(scenes[scene]['read_only']['unchanged'], f'场景 {scene} 读取改动了副本行')
        expect(scenes[scene]['read_only']['source_unchanged_bytewise'],
               f'场景 {scene} 读取改动了原库文件')
        expect(scenes[scene]['database']['digest'] == findings['database_digests'][scene],
               f'场景 {scene} 摘要不一致')
        expect(scenes[scene]['label'], f'场景 {scene} 缺少说明')
        expect(scenes[scene]['head_evidence']['observed_at'] == OBSERVED, f'场景 {scene} 观察时间不符')
        expect(scenes[scene]['successor_evidence']['trace'] == scenes[scene]['successor_trace'],
               f'场景 {scene} 后继证据与轨迹不一致')
    seen = {item['id'] for item in bundle['experiments']}
    expect(seen == {name for name, *_ in EXPERIMENTS}, '副本实验集合不完整')
    for item in bundle['experiments']:
        expect(item['findings'], f"{item['id']} 没有结论")
        for key, value in item['findings'].items():
            expect(value is True, f"{item['id']} 的 {key} 不为真")
        expect(item['database_digest_before'] == findings['database_digests'][item['source']],
               f"{item['id']} 的副本起点与场景 {item['source']} 摘要不符")
        expect(item['steps'], f"{item['id']} 没有步骤")


def verify(stored_dir):
    with TemporaryDirectory(prefix='quaybell-triage-verify-') as directory:
        rebuilt = build_bundle(Path(directory))
    validate(rebuilt)
    stored_path = Path(stored_dir) / 'bundle.json'
    stored = json.loads(stored_path.read_text(encoding='utf-8'))
    stored_digest = stored.pop('sha256')
    rebuilt_digest = rebuilt.pop('sha256')
    expect(fingerprint(pack(stored)) == stored_digest, '已保存的取证包摘要自校验失败')
    expect(fingerprint(pack(rebuilt)) == rebuilt_digest, '重建取证包摘要自校验失败')
    expect(pack(stored) == pack(rebuilt), '重建结果与已保存的取证包不一致')
    checked = 0
    for scene in ('a', 'b'):
        for role in ('head', 'successor'):
            name = f'scene-{scene}-{role}.evidence.json'
            raw = (Path(stored_dir) / name).read_text(encoding='ascii')
            record = check_evidence(raw.encode('ascii'))
            expect(record == stored['scenes'][scene][f'{role}_evidence'], f'{name} 与取证包不一致')
            expect(raw == stored['scenes'][scene][f'{role}_evidence_raw'], f'{name} 原文不一致')
            checked += 1
    print(f'verified {stored_path} ({checked} 份证据导出，sha256 {stored_digest[:16]}…)')
    return stored


def export(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix='quaybell-triage-build-') as work:
        bundle = build_bundle(Path(work))
    (directory / 'bundle.json').write_text(json.dumps(bundle, ensure_ascii=False, indent=2) + '\n',
                                           encoding='utf-8')
    for scene in ('a', 'b'):
        for role in ('head', 'successor'):
            (directory / f'scene-{scene}-{role}.evidence.json').write_text(
                bundle['scenes'][scene][f'{role}_evidence_raw'], encoding='ascii')
    return bundle


def summary(bundle):
    lines = [f"取证包 {bundle['format']} · sha256 {bundle['sha256']}", '']
    for scene in ('a', 'b'):
        record = bundle['scenes'][scene]
        counts = record['group']['counts']
        lines.append(f"场景 {scene}：{record['label']}")
        lines.append(f"  paused={record['group']['paused']} counts={json.dumps(counts, ensure_ascii=False, sort_keys=True)}")
        lines.append(f"  队头 {record['head_facts']['state']}，尝试={[item['outcome'] for item in record['head_facts']['attempts']]}，ready_at={record['head_facts']['ready_at']}")
        lines.append(f"  看板样本 {len(record['group']['sample'])} 行，目标投递 {record['delivery_ids']} 不在样本中")
        lines.append(f"  行级摘要 {record['database']['digest'][:16]}…"
                     f"（副本行不变 {record['read_only']['unchanged']}，"
                     f"原库文件字节不变 {record['read_only']['source_unchanged_bytewise']}）")
        lines.append('')
    lines.append('副本实验：')
    for item in bundle['experiments']:
        lines.append(f"  {item['id']}：{item['label']}")
        for key, value in item['findings'].items():
            lines.append(f"    {key} = {json.dumps(value, ensure_ascii=False)}")
    lines.append('')
    lines.append('看板完整响应中的区分：')
    lines.append(f"  样本是否一致 = {bundle['distinguishers']['sample_identical']}")
    lines.append(f"  计数差异 = {json.dumps(bundle['distinguishers']['counts_differ'], ensure_ascii=False)}")
    lines.append(f"  队头尝试结果 = {json.dumps(bundle['distinguishers']['head_attempt_outcome'], ensure_ascii=False)}")
    lines.append(f"  后继事实是否一致 = {bundle['distinguishers']['successor_facts_identical']}")
    lines.append('')
    lines.append('如果接班人只拿到看板样本，以下结论必须保留：')
    for item in bundle['sample_only_caveats']:
        lines.append(f'  - {item}')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='复原并复核两种看板相同的积压现场')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--export', metavar='DIR', type=Path, help='写出取证包和证据导出')
    group.add_argument('--verify', metavar='DIR', type=Path, help='重建现场并比对已保存的取证包')
    args = parser.parse_args()
    if args.verify:
        verify(args.verify)
        return
    if args.export:
        bundle = export(args.export)
        validate(bundle)
        print(f'wrote {args.export} ({bundle["sha256"][:16]}…)')
    else:
        with TemporaryDirectory(prefix='quaybell-triage-build-') as work:
            bundle = build_bundle(Path(work))
        validate(bundle)
    print(summary(bundle))


if __name__ == '__main__':
    sys.exit(main())
