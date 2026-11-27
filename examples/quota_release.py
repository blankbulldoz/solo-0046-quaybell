"""频道消息配额释放演练：在真实 HTTP 实例上用受控时钟复算释放量表。

场景（Unix 秒，T0 = 1_800_000_000；策略同 --demo：租约 90s、退避 20s、保留 7200s）：

  消息 A（T0+20 发布，T0+7220 到保留期）
      本组已确认；合作方那份 T0+7220 才领，租约到 T0+7310，横跨两个审核时点。
  消息 B（T0+10 发布，T0+7210 到保留期）
      本组终败进死信；重投已追加一个本组未领的后继；合作方那份已确认。
  消息 C1..C4（T0+100 发布，T0+7300 到保留期）
      同一批告警同时分给两组；两组 T0+7245 各领 4 条，租约到 T0+7335。
      保留期落在 T1 与 T2 之间，租约到期在 T2 之后。

审核时点 T1 = T0+7250，T2 = T0+7325；T2+15 是约定时刻之后的补救点。

保留规则（quaybell/retention.py::eligible）按“消息条数”判定：年龄与发布时绑定
的策略版本比较，且整条消息的所有派生投递都不得处于 waiting/leased。
释放量的单位因此是消息条数；删行只把页标成空闲，文件字节要 VACUUM 才缩。

每个清理点都先用 GET /channels/{c}/retention 预查，再执行 POST 清理，并断言
预查名单与实际删除完全一致；同样的谓词会以只读 SQL 逐条重放，列出卡住的行。

    python3 -m examples.quota_release                  # 演示策略 max_attempts=1
    python3 -m examples.quota_release --attempts 2     # 给合作方留一次重试预算
    python3 -m examples.quota_release --json out.json
"""
import argparse
import json
import os
import sqlite3
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from quaybell.persistence import RelayPersistence
from quaybell.server import RelayRequest, RelayServer

T0 = 1_800_000_000.0
T1 = T0 + 7250.0
T2 = T0 + 7325.0
PROBE = T0 + 7340.0
A_LEASE_END = T0 + 7310.0
C_LEASE_END = T0 + 7335.0
CHANNEL = 'signal-weir'
BASE = f'/channels/{CHANNEL}'
DEMO = dict(lease_seconds=90, retry_seconds=20, max_attempts=1, retention_seconds=7200)
BODY = 'x' * 3000            # 一条消息占满一页，字节对比才有意义
TAGS = {'feed': 'telemetry'}

RelayRequest.log_message = lambda *args, **kwargs: None


class Client:
    def __init__(self, server, clock):
        self.base = f'http://127.0.0.1:{server.server_port}/v1'
        self.clock = clock

    def call(self, method, path, data=None, expected=200):
        kw = dict(method=method)
        if data is not None:
            kw['data'] = json.dumps(data).encode('utf-8')
            kw['headers'] = {'Content-Type': 'application/json'}
        try:
            response = urlopen(Request(self.base + path, **kw), timeout=10)
        except HTTPError as exc:
            response = exc
        with response:
            body = json.load(response)
            if expected is not None and response.code != expected:
                raise RuntimeError((method, path, response.code, expected, body))
            return body

    def get(self, path, expected=200):
        return self.call('GET', path, None, expected)

    def post(self, path, data=None, expected=200):
        return self.call('POST', path, {} if data is None else data, expected)

    def at(self, moment):
        assert moment >= self.clock[0], (moment, self.clock[0])   # 时钟只许前进
        self.clock[0] = float(moment)


def publish(client, key, lane):
    return client.post(BASE + '/messages', dict(producer='pulse-encoder', key=key, lane=lane,
                                                tags=TAGS, payload={'body': BODY}), 201)


def claim(client, group, limit):
    return client.post(f'{BASE}/groups/{group}/claim',
                       {'worker': group, 'limit': limit})['items']


def ack(client, item, expected=200):
    return client.post(f"/deliveries/{item['delivery_id']}/ack",
                       {'token': item['lease']['token']}, expected)


def nack(client, item, reason='operator-drill'):
    return client.post(f"/deliveries/{item['delivery_id']}/nack",
                       {'token': item['lease']['token'], 'reason': reason})


def preview(client):
    return [row['id'] for row in client.get(BASE + '/retention')['eligible']]


def sweep(client):
    return [(row['delivery_id'], row['state'], row['effective_at'])
            for row in client.post('/sweep', {'limit': 100})['recovered']]


def checkpoint(client):
    """预查 → 清理 → 断言两者一致；返回实际释放的消息 ID。"""
    planned = preview(client)
    removed = client.post(BASE + '/retention', {})['removed']
    assert planned == removed, (planned, removed)
    return removed


def audit(path):
    con = sqlite3.connect(path, timeout=10)
    try:
        return [dict(removed=json.loads(row[0])) for row in con.execute(
            "SELECT details FROM journal WHERE kind='pruned' ORDER BY id")]
    finally:
        con.close()


def revision_is_not_retroactive():
    """把 retention 改小并不能清掉旧消息：年龄按发布时绑定的版本算。

    返回两次观察：新策略生效 60s 后（只有 v2 消息到期），以及新的
    retention 也早已超过旧消息年龄时（旧消息才按自己 v1 的 7200s 到期）。
    """
    tmp = TemporaryDirectory(prefix='quota-revision-')
    path = Path(tmp.name) / 'relay.sqlite3'
    clock = [T0]
    server, thread = serve(path, clock)
    client = Client(server, clock)
    retro = '/channels/retro'
    try:
        def send(key, lane):
            return client.post(retro + '/messages', dict(
                producer='pulse-encoder', key=key, lane=lane,
                tags=TAGS, payload={'body': BODY}), 201)

        def drain():
            for row in client.post(retro + '/groups/g/claim',
                                   {'worker': 'g', 'limit': 5})['items']:
                ack(client, row)

        client.at(T0)
        client.post('/channels', {'name': 'retro', 'policy': DEMO}, 201)
        client.post(retro + '/groups', {'name': 'g'}, 201)
        old = send('retro-old', 'coil-r1')
        drain()
        client.at(T0 + 10)
        revision = client.post(retro + '/policy',
                               DEMO | {'retention_seconds': 60, 'expected': 1}, 201)
        new = send('retro-new', 'coil-r2')
        drain()
        client.at(T0 + 70)
        soon = [row['id'] for row in client.get(retro + '/retention')['eligible']]
        client.at(T0 + 7200)
        later = [row['id'] for row in client.get(retro + '/retention')['eligible']]
        return dict(revision=revision['version'], old=old['message_id'], new=new['message_id'],
                    at_new_retention_elapsed=soon, at_old_retention_elapsed=later)
    finally:
        stop(server, thread)
        tmp.cleanup()


def metrics(path):
    con = sqlite3.connect(path, timeout=10)
    try:
        try:
            con.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        except sqlite3.OperationalError:
            pass
        page = con.execute('PRAGMA page_size').fetchone()[0]
        pages = con.execute('PRAGMA page_count').fetchone()[0]
        free = con.execute('PRAGMA freelist_count').fetchone()[0]
        try:
            tables = {name: int(size) for name, size in con.execute(
                'SELECT name, sum(pgsize) FROM dbstat GROUP BY name')}
        except sqlite3.OperationalError:
            tables = {}
        rows = {name: con.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
                for name in ('messages', 'deliveries', 'attempts', 'journal')}
        wal = Path(str(path) + '-wal')
        return dict(file_bytes=os.path.getsize(path),
                    wal_bytes=wal.stat().st_size if wal.exists() else 0,
                    page_size=page, pages=pages, freelist_pages=free,
                    live_bytes=(pages - free) * page,
                    messages_bytes=tables.get('messages', 0),
                    rows=rows,
                    channel_messages=con.execute(
                        'SELECT count(*) FROM messages WHERE channel=?', (CHANNEL,)).fetchone()[0])
    finally:
        con.close()


def ledger(path, now):
    """只读重放 retention.eligible 的谓词，逐条列出到期时间与卡住它的投递。"""
    con = sqlite3.connect(path, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        result = []
        for row in con.execute('''
                SELECT m.id, m.published, p.version AS policy, p.retention_seconds,
                       m.published + p.retention_seconds AS due
                FROM messages m
                JOIN policies p ON p.channel=m.channel AND p.version=m.policy
                WHERE m.channel=? ORDER BY m.id''', (CHANNEL,)):
            held = []
            for item in con.execute('''
                    SELECT d.id AS delivery, s.name AS grp, d.state, d.attempt_count,
                           a.expires AS open_until
                    FROM deliveries d
                    JOIN subscriptions s ON s.id=d.subscription_id
                    LEFT JOIN attempts a ON a.delivery_id=d.id AND a.outcome='open'
                    WHERE d.message_id=? AND d.state IN ('waiting','leased')
                    ORDER BY d.id''', (row['id'],)):
                held.append(f"{item['grp']}:{item['state']}"
                            + (f"@{item['open_until']:.0f}" if item['open_until'] else ''))
            result.append(dict(id=row['id'], published=row['published'], policy=row['policy'],
                               retention=row['retention_seconds'], due=row['due'],
                               aged=row['due'] <= now, held=held,
                               eligible=row['due'] <= now and not held))
        return result
    finally:
        con.close()


def build(client, attempts):
    """建立 A / B / C 三条线索，停在 T1 之前的公共状态。"""
    client.at(T0)
    client.post('/channels', {'name': CHANNEL, 'policy': DEMO}, 201)
    client.post(BASE + '/groups', {'name': 'receipt-rotor'}, 201)
    client.post(BASE + '/groups', {'name': 'drift-keeper', 'tags': TAGS}, 201)

    # B：v1 下本组终败；合作方那份确认掉。
    client.at(T0 + 10)
    b = publish(client, 'pulse-b', 'coil-b')
    b_root = claim(client, 'receipt-rotor', 1)[0]
    assert b_root['message_id'] == b['message_id']
    assert nack(client, b_root, 'terminal-decoder-error')['state'] == 'dead'

    if attempts != DEMO['max_attempts']:
        client.at(T0 + 12)
        client.post(BASE + '/policy', DEMO | {'max_attempts': attempts, 'expected': 1}, 201)

    # A：本组已确认；合作方那份 T0+7220 才领，租约到 T0+7310。
    client.at(T0 + 20)
    a = publish(client, 'pulse-a', 'coil-a')
    client.at(T0 + 25)
    a_ours = {row['message_id']: row for row in claim(client, 'receipt-rotor', 1)}[a['message_id']]
    ack(client, a_ours)
    b_theirs = {row['message_id']: row for row in claim(client, 'drift-keeper', 1)}[b['message_id']]
    ack(client, b_theirs)

    # B 的重投后继留在本组未领。
    client.at(T0 + 30)
    successor = client.post(f"/deliveries/{b_root['delivery_id']}/redrive", {}, 201)

    # C：四个告警同时分给两组。
    client.at(T0 + 100)
    c = [publish(client, f'pulse-c{i}', f'coil-c{i}') for i in range(1, 5)]

    client.at(T0 + 7220)
    a_theirs = {row['message_id']: row for row in claim(client, 'drift-keeper', 1)}[a['message_id']]
    assert a_theirs['lease']['expires'] == A_LEASE_END, a_theirs
    return dict(A=a['message_id'], B=b['message_id'], C=[row['message_id'] for row in c],
                successor=successor['delivery_id'], a_theirs=a_theirs,
                b_root=b_root['delivery_id'])


class Branch:
    """partner_a: none|ack|retry|late
    own_c / partner_c: all|none|first_half|second_half（各自在 T2 结清哪些 C）
    partner_c_late: 合作方拖到补救点才确认 C（租约已过）
    partner_c_reclaim: 合作方在补救点回收后重领并确认 C
    own_successor：本组是否领并确认 B 的后继
    sweep_t2 / sweep_probe：是否在 T2 / 补救点执行 /sweep"""
    def __init__(self, name, partner_a='none', own_c='all', partner_c='none',
                 partner_c_late=False, partner_c_reclaim=False, partner_c_retry=False,
                 own_successor=True, own_limit=5, sweep_t2=True, sweep_probe=True):
        self.name = name
        self.partner_c_retry = partner_c_retry
        self.own_limit = own_limit
        self.partner_a = partner_a
        self.own_c = own_c
        self.partner_c = partner_c
        self.partner_c_late = partner_c_late
        self.partner_c_reclaim = partner_c_reclaim
        self.own_successor = own_successor
        self.sweep_t2 = sweep_t2
        self.sweep_probe = sweep_probe


def serve(path, clock):
    server = RelayServer(('127.0.0.1', 0), RelayPersistence(path), lambda: clock[0])
    thread = threading.Thread(target=server.serve_forever,
                              kwargs={'poll_interval': .01}, daemon=True)
    thread.start()
    return server, thread


def stop(server, thread):
    server.shutdown()
    thread.join()


def subset(kind, ids):
    return {'all': ids, 'none': [], 'first_half': ids[:2], 'second_half': ids[2:]}[kind]


def run(branch, attempts):
    tmp = TemporaryDirectory(prefix=f'quota-{branch.name}-')
    path = Path(tmp.name) / 'relay.sqlite3'
    clock = [T0]
    server, thread = serve(path, clock)
    client = Client(server, clock)
    out = dict(branch=branch.name, attempts=attempts)
    try:
        ids = build(client, attempts)
        out['ids'] = ids
        out['ledger_at_7249'] = ledger(path, T0 + 7249)
        out['metrics_before'] = metrics(path)

        # 本组动作 1（不依赖合作方）：领走并确认 B 的重投后继。
        client.at(T0 + 7240)
        if branch.own_successor:
            item = claim(client, 'receipt-rotor', 1)[0]
            assert item['delivery_id'] == ids['successor'], item
            out['own_successor'] = ack(client, item)['state']
        else:
            out['own_successor'] = 'left-waiting'

        # 两组同时领走 C。本组若没先结清 B 的后继，批量领取会顺手把它带走
        # （领取按投递 ID 排序），所以本组多领一条以免 C 少一条。
        client.at(T0 + 7245)
        limit = branch.own_limit if not branch.own_successor else 4
        held = {'receipt-rotor': claim(client, 'receipt-rotor', limit),
                'drift-keeper': claim(client, 'drift-keeper', 4)}
        assert len(held['drift-keeper']) == 4, held['drift-keeper']
        assert {row['lease']['expires'] for rows in held.values() for row in rows} == {C_LEASE_END}
        out['c_claimed'] = {g: sorted(row['message_id'] for row in rows) for g, rows in held.items()}
        out['successor_claimed_with_batch'] = any(
            row['delivery_id'] == ids['successor'] for row in held['receipt-rotor'])
        # 凭据不进输出：每次运行随机，会让结果无法逐字节比对；证据导出也不含凭据。
        out['ids'] = {key: (value if key != 'a_theirs' else
                            {k: v for k, v in value.items() if k != 'lease'})
                      for key, value in ids.items()}

        # ---- 审核点 1 ----
        client.at(T1)
        out['t1_ledger'] = ledger(path, T1)
        if branch.partner_a == 'ack':
            out['a_result'] = ack(client, ids['a_theirs'])['state']
        elif branch.partner_a in ('retry', 'retry_abandon'):
            outcome = nack(client, ids['a_theirs'], 'partner-wants-retry')
            out['a_retry_state'] = outcome['state']
            out['a_retry_effective'] = outcome['effective_at']
            out['a_result'] = outcome['state']
        else:
            out['a_result'] = 'none'
        out['t1_preview'] = preview(client)
        out['t1_metrics'] = metrics(path)
        out['t1_pruned'] = checkpoint(client)
        out['t1_metrics_after_prune'] = metrics(path)

        # ---- T1 与 T2 之间 ----
        if branch.partner_a == 'retry' and out['a_retry_state'] == 'waiting':
            client.at(out['a_retry_effective'] + 1)
            again = {row['message_id']: row for row in claim(client, 'drift-keeper', 1)}
            out['a_retry_ack'] = ack(client, again[ids['A']])['state']
        if branch.partner_a == 'late':
            client.at(T0 + 7315)                      # 租约 7310 已过
            out['a_result'] = ack(client, ids['a_theirs'], expected=None)['error']

        # ---- 审核点 2 ----
        client.at(T2)
        out['sweep_t2'] = sweep(client) if branch.sweep_t2 else 'skipped'
        own_set = set(subset(branch.own_c, ids['C']))
        partner_set = set(subset(branch.partner_c, ids['C']))
        done = {'receipt-rotor': [], 'drift-keeper': []}
        for group, rows in held.items():
            wanted = own_set if group == 'receipt-rotor' else partner_set
            for row in rows:
                if row['message_id'] in wanted:
                    if group == 'drift-keeper' and branch.partner_c_retry:
                        outcome = nack(client, row, 'partner-retries-instead')
                        out.setdefault('c_retry_states', []).append(outcome['state'])
                    else:
                        ack(client, row)
                    done[group].append(row['message_id'])
        out['c_acked'] = {g: sorted(v) for g, v in done.items()}
        out['t2_ledger'] = ledger(path, T2)
        out['t2_preview'] = preview(client)
        out['t2_metrics'] = metrics(path)
        out['t2_pruned'] = checkpoint(client)
        out['t2_metrics_after_prune'] = metrics(path)

        # ---- 约定时刻之后：补救 ----
        out['c_late'] = 'skipped'
        if branch.partner_c_late:
            client.at(PROBE - 10 if branch.partner_c_late == 'near' else PROBE)
            leftover = [row for row in held['drift-keeper']
                        if row['message_id'] not in set(done['drift-keeper'])]
            out['c_late'] = [ack(client, row, expected=None).get('error', 'accepted')
                             for row in leftover] or 'nothing-left'
        client.at(PROBE)
        if branch.partner_c_reclaim:
            # 租约 7335 已过：回收把 open 尝试判超时，退避从原截止起算；
            # 退避结束后重新领取并确认（这是新一次尝试，不是迟到确认）。
            out['probe_reclaim_sweep'] = sweep(client)
            back = [row for row in claim(client, 'drift-keeper', 4) if row['message_id'] in ids['C']]
            if not back:                         # 演示策略下已直接终态，无需重试
                client.at(PROBE + 25)
                back = [row for row in claim(client, 'drift-keeper', 4)
                        if row['message_id'] in ids['C']]
            out['c_reclaim'] = [ack(client, row)['state'] for row in back]
            client.at(PROBE + 30)
        out['sweep_probe'] = sweep(client) if branch.sweep_probe else 'skipped'
        out['probe_ledger'] = ledger(path, PROBE + 30)
        out['probe_preview'] = preview(client)
        out['probe_pruned'] = checkpoint(client)
        out['probe_metrics'] = metrics(path)
        out['audit'] = audit(path)
    finally:
        stop(server, thread)

    con = sqlite3.connect(path)
    con.execute('VACUUM')
    con.close()
    out['vacuum_metrics'] = metrics(path)

    # 清理过的消息应释放生产键；没清理的复用原回执。
    server, thread = serve(path, clock)
    try:
        again = Client(server, clock).post(BASE + '/messages', dict(
            producer='pulse-encoder', key='pulse-b', lane='coil-b',
            tags=TAGS, payload={'body': BODY}), expected=None)
    finally:
        stop(server, thread)
    out['republish_b'] = {'message_id': again['message_id'],
                          'repeated': again['repeated'],
                          'reused_key': again['message_id'] == ids['B']}
    tmp.cleanup()
    return out


BRANCHES = [
    Branch('R1 本组动作下界'),
    Branch('R2 下界：不结清 B 的后继', own_successor=False),
    Branch('R2b 下界：只按 4 条领 C', own_successor=False, own_limit=4),
    Branch('R2c 漏领 1 条 C＋合作方全结清', own_successor=False, own_limit=4,
           partner_a='ack', partner_c='all'),
    Branch('R3 下界：T2 不做回收', sweep_t2=False),
    Branch('R4 合作方 T1 确认 A', partner_a='ack'),
    Branch('R5 合作方 T1 确认 A、T2 结清 C', partner_a='ack', partner_c='all'),
    Branch('R6 合作方改为重试 A', partner_a='retry', partner_c='all'),
    Branch('R7 合作方确认 A 迟到（仍回收）', partner_a='late', partner_c='all'),
    Branch('R8 合作方确认 A 迟到且漏回收', partner_a='late', partner_c='all', sweep_t2=False),
    Branch('R9 合作方补救点才确认 C', partner_a='ack', partner_c_late=True),
    Branch('R9b 合作方 T2 后 10s 内确认 C', partner_a='ack', partner_c_late='near'),
    Branch('R10 合作方补救点重领确认 C', partner_a='ack', partner_c_reclaim=True),
    Branch('R11 各结清一半（同一半）', partner_a='ack', own_c='first_half',
           partner_c='first_half'),
    Branch('R12 各结清一半（不同半）', partner_a='ack', own_c='first_half',
           partner_c='second_half'),
    Branch('R13 合作方重试 A 后不再回', partner_a='retry_abandon', partner_c='all'),
    Branch('R14 合作方对 C 改为重试', partner_a='ack', partner_c='all',
           partner_c_retry=True),
]


def summarize(rows, need):
    print(f"{'分支':30} {'T1':>3} {'T2':>3} {'T2累计':>6} {'缺口':>4} {'补救':>4} {'A结果':9} "
          f"{'C结清':>5} {'剩消息':>4} {'文件KB':>6} {'消息表KB':>7} {'空闲页':>4}")
    for row in rows:
        t1, t2 = len(row['t1_pruned']), len(row['t2_pruned'])
        m = row['t2_metrics_after_prune']
        gap = max(0, need - (t1 + t2))
        print(f"{row['branch']:30} {t1:>3} {t2:>3} {t1 + t2:>6} {gap:>4} "
              f"{len(row['probe_pruned']):>4} {str(row['a_result']):9} "
              f"{sum(len(v) for v in row['c_acked'].values()):>5} "
              f"{m['channel_messages']:>4} {m['file_bytes'] / 1024:>6.0f} "
              f"{m['messages_bytes'] / 1024:>7.0f} {m['freelist_pages']:>4}")


def admission(rows, need, moment):
    """在某个审核时点，哪些分支腾出的条数够接 need 条新告警。"""
    field_t1 = 't1_pruned'
    field_t2 = 't2_pruned'
    passed, failed = [], []
    for row in rows:
        freed = len(row[field_t1]) if moment == 'T1' else len(row[field_t1]) + len(row[field_t2])
        (passed if freed >= need else failed).append((row['branch'], freed))
    return passed, failed


def print_ledger(row, moment, items):
    print(f"  {moment} 逐条账：")
    for item in items:
        held = '、'.join(item['held']) or '—'
        mark = '可释放' if item['eligible'] else '不可释放'
        print(f"    消息 {item['id']}（v{item['policy']}，到期 {item['due']:.0f}，"
              f"{'已满龄' if item['aged'] else '未满龄'}）卡在：{held} → {mark}")


def print_disk(row):
    print('  磁盘（同一分支，逐检查点）：')
    for tag, label in (('metrics_before', '清理前'), ('t1_metrics_after_prune', 'T1 清理后'),
                       ('t2_metrics_after_prune', 'T2 清理后'), ('probe_metrics', '补救后'),
                       ('vacuum_metrics', 'VACUUM 后')):
        m = row[tag]
        print(f"    {label:9} 文件 {m['file_bytes']:>7}B  WAL {m['wal_bytes']:>6}B  "
              f"页 {m['pages']:>3}（空闲 {m['freelist_pages']:>2}）  "
              f"消息表 {m['messages_bytes']:>6}B  消息行 {m['channel_messages']}")


def main():
    parser = argparse.ArgumentParser(description='复算频道消息配额的释放量表')
    parser.add_argument('--attempts', type=int, default=DEMO['max_attempts'],
                        help='频道策略 max_attempts（演示默认 1；2 表示给合作方留一次重试）')
    parser.add_argument('--need', type=int, default=4,
                        help='新告警批次的条数（默认与 C 批同规模：4）')
    parser.add_argument('--json', type=Path, help='把每个分支的完整结果写入 JSON')
    args = parser.parse_args()
    rows = [run(branch, args.attempts) for branch in BRANCHES]
    print(f'策略：{DEMO | {"max_attempts": args.attempts}}  T1={T1:.0f}  T2={T2:.0f}  '
          f'A 租约止={A_LEASE_END:.0f}  C 租约止={C_LEASE_END:.0f}  '
          f'新告警批次={args.need} 条')
    summarize(rows, args.need)
    base = rows[0]
    print(f'\nR1（不靠合作方）的卡点：')
    print_ledger(base, 'T1', base['t1_ledger'])
    print_ledger(base, 'T2', base['t2_ledger'])
    print('\nR1 的消息配额 vs 磁盘字节：')
    print_disk(base)
    full = next(row for row in rows if row['branch'].startswith('R5'))
    print('\nR5（合作方配合到底）的消息配额 vs 磁盘字节：')
    print_disk(full)
    for moment in ('T1', 'T2'):
        passed, failed = admission(rows, args.need, moment)
        print(f'\n{moment} 能否接 {args.need} 条新告警：'
              f'够 {"、".join(f"{name}({freed})" for name, freed in passed) or "无"}')
        print(f'  不够（会进不来）：{"、".join(f"{name}({freed})" for name, freed in failed)}')
    retro = revision_is_not_retroactive()
    print(f"\n把 retention 改小不追溯旧消息（{retro}）："
          f"新策略满 60s 时只释放 {retro['at_new_retention_elapsed']}，"
          f"旧消息要到自身 v1 的 7200s 才进 {retro['at_old_retention_elapsed']}。")
    if args.json:
        args.json.write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding='utf-8')


if __name__ == '__main__':
    main()
