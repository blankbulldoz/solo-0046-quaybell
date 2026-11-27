"""Rehearse both proposed consumer cut-over windows against a real local relay.

Drives the public HTTP listener only (the same entry points the console and
``examples.walkthrough`` use), with a controlled clock so every lease deadline,
backoff and retention cutoff is reproducible. Nothing outside a temp SQLite
file is written.

Per window two switch styles are rehearsed:

``freeze``  the old fleet is decommissioned at the cut and stops acking, so
            every lease it held lapses and -- under ``max_attempts=1`` -- goes
            straight to dead letters.
``paced``   the group is paused at the cut, the old fleet keeps acking the
            leases it already holds at its observed pace, and whatever it
            cannot settle before its lease deadline becomes dead letters.

Cross-checks run once:

``lease_extension_check``  a policy revision cannot lengthen leases already
                           handed out (only future claims get the new value).
``rollback_check``         pause/unpause is not bound to a fleet: after a
                           pause the old workers can claim again, so an
                           abandoned switch does not need new tokens.
``bounded_sweep_check``    global recovery honours its limit and orders by the
                           original deadline, so a full sweep may take several
                           calls inside a ten-minute window.

Usage:
    python3 -I docs/window_rehearsal.py                    # both windows
    python3 -I docs/window_rehearsal.py early              # 09:00 only
    python3 -I docs/window_rehearsal.py heavy --json /tmp/facts.json
    python3 -I docs/window_rehearsal.py --markdown docs/window-rehearsal-record.md
"""
import argparse
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from quaybell.persistence import RelayPersistence  # noqa: E402
from quaybell.server import RelayRequest, RelayServer  # noqa: E402

# Rehearsal clock anchors: 2026-11-30 (Monday), +08:00, Unix seconds.
WINDOW_START = {'early': 1_795_741_200.0, 'heavy': 1_795_743_600.0}
LABEL = {'early': '09:00', 'heavy': '09:40'}

# Production-shaped policy under test: one delivery attempt, 90 s lease.
# max_attempts=1 is deliberate: an expired lease has no retry budget and lands
# in dead letters, so every lapsed lease is manual debt for the next shift.
POLICY = dict(lease_seconds=90, retry_seconds=20, max_attempts=1, retention_seconds=7200)
LEASE = POLICY['lease_seconds']
OLD_CLAIM_AT = -30.0      # old fleet's last claim; leases then expire at cut+60
CUT_AT = 0.0
WINDOW_END = 600.0        # consumer team's ten-minute switch budget
DRIFT_GROUP = 'drift-keeper'

# Observed old-fleet ack cadence (ops notes), as (offset from cut, slice size).
# 09:00: low traffic, idle workers -> 3 keys per 10 s, all 18 clear by cut+55.
# 09:40: high traffic, warm workers -> bulk of 21 clears by cut+20, but the
#        long tail of 3 does not clear before the lease deadline.
ACK_CADENCE = {
    'early': [(5, 3), (15, 3), (25, 3), (35, 3), (45, 3), (55, 3)],
    'heavy': [(20, 17), (70, 3)],
}
# Traffic that keeps arriving after the drain, inside the ten-minute window.
ARRIVALS = {'early': [(150, 6)], 'heavy': [(120, 6), (240, 6), (360, 6)]}
# Pre-window shape: lanes x depth, plus how many generations the old fleet
# already settled before the cut.
SHAPE = {'early': dict(lanes=18, depth=3, settled=0),
         'heavy': dict(lanes=20, depth=4, settled=2)}


class QuietRequest(RelayRequest):
    def log_message(self, fmt, *args):
        pass


class QuietServer(RelayServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.RequestHandlerClass = QuietRequest


class Client:
    def __init__(self, server, clock, channel, group='consumer'):
        self.base = f'http://127.0.0.1:{server.server_port}/v1'
        self.clock, self.channel, self.group = clock, channel, group
        self.last_seen = -float('inf')

    def set_clock(self, value):
        if value < self.last_seen:
            raise RuntimeError(f'clock regression {value} < {self.last_seen}')
        self.clock[0] = value
        self.last_seen = value

    def req(self, path, data=None, expected=200, raw=False):
        info = {} if data is None else dict(
            data=json.dumps(data).encode('utf-8'),
            headers={'Content-Type': 'application/json'})
        try:
            response = urlopen(Request(self.base + path, **info), timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            body = response.read()
            if response.code != expected:
                raise RuntimeError((path, response.code, expected, body[:300]))
            return body if raw else json.loads(body)

    def post(self, path, data=None, expected=200):
        return self.req(path, data, expected)

    def get(self, path, expected=200, raw=False):
        return self.req(path, None, expected, raw)

    def setup(self):
        self.post('/channels', {'name': self.channel, 'policy': POLICY}, 201)
        for name, tags in (('consumer', {}), (DRIFT_GROUP, {'feed': 'telemetry'})):
            self.post(f'/channels/{self.channel}/groups', {'name': name, 'tags': tags}, 201)
        self.post(f'/channels/{self.channel}/groups/{DRIFT_GROUP}/pause', {'paused': True})

    def publish(self, key, lane, tags=None, delay=0):
        return self.post(f'/channels/{self.channel}/messages',
                         dict(producer='drill', key=key, lane=lane, payload={'k': key},
                              tags=tags or {'feed': 'samples'}, delay_seconds=delay), 201)

    def claim(self, limit=30, worker='edge-old', group=None):
        return self.post(f'/channels/{self.channel}/groups/{group or self.group}/claim',
                         {'worker': worker, 'limit': limit})

    def ack(self, item, expected=200):
        return self.post(f"/deliveries/{item['delivery_id']}/ack",
                         {'token': item['lease']['token']}, expected)

    def pause(self, state, group=None):
        return self.post(f'/channels/{self.channel}/groups/{group or self.group}/pause',
                         {'paused': state})

    def board(self):
        return self.get(f'/channels/{self.channel}')

    def sweep(self, limit=100):
        return self.post('/sweep', {'limit': limit})

    def dead(self):
        return self.get(f'/channels/{self.channel}/dead')['items']

    def retention(self):
        return self.get(f'/channels/{self.channel}/retention')

    def trace(self, message_id):
        return self.get(f'/messages/{message_id}')

    def evidence(self, message_id):
        return self.get(f'/messages/{message_id}/evidence', raw=True)

    def counts(self, group='consumer'):
        board = self.board()
        return next(g for g in board['groups'] if g['name'] == group)['counts'], board

    def drain(self, group, worker, step=5, cap=40):
        batches = settled = 0
        while True:
            items = self.claim(30, worker=worker, group=group)['items']
            if not items:
                return batches, settled
            batches += 1
            for item in items:
                self.ack(item)
                settled += 1
            self.set_clock(self.clock[0] + step)
            if batches > cap:
                raise RuntimeError('drain did not settle')


def observe(client, log, offset, action, **extra):
    counts, board = client.counts()
    drift, _ = client.counts(DRIFT_GROUP)
    row = dict(offset=round(offset, 1), at=client.clock[0], action=action, counts=counts,
               drift_counts=drift, dead=len(board['dead']), observed_at=board['observed_at'],
               policy_version=board['policy']['version'])
    row.update(extra)
    log.append(row)
    return row


def publish_batch(client, lanes, depth, prefix, telemetry_every=2):
    for index in lanes:
        lane = f'lane-{index:02d}'
        for n in range(depth):
            tags = {'feed': 'telemetry'} if (index + n) % telemetry_every == 0 else {'feed': 'samples'}
            client.publish(f'{prefix}-{lane}-{n}', lane, tags=tags)


def run_window(name, variant):
    clock = [WINDOW_START[name] - 120]
    log, facts = [], {'window': name, 'variant': variant, 'label': LABEL[name]}
    channel = f'drill-{name}-{variant}'
    shape = SHAPE[name]
    with TemporaryDirectory(prefix='quaybell-drill-') as directory:
        persistence = RelayPersistence(Path(directory) / 'relay.sqlite3')
        with QuietServer(('127.0.0.1', 0), persistence, lambda: clock[0]) as server:
            thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
            thread.start()
            try:
                client = Client(server, clock, channel)
                client.setup()
                client.set_clock(WINDOW_START[name] - 120)
                lanes = range(1, shape['lanes'] + 1)
                publish_batch(client, lanes, shape['depth'], name)
                facts['published_before_cut'] = shape['lanes'] * shape['depth']
                observe(client, log, -120, f"发布 {facts['published_before_cut']} 条"
                        f"（{shape['lanes']} 个顺序键 × {shape['depth']}）")
                settled = 0
                for generation in range(shape['settled']):
                    client.set_clock(WINDOW_START[name] - 100 + generation * 10)
                    batch = client.claim(30)['items']
                    client.set_clock(client.clock[0] + 5)
                    for item in batch:
                        client.ack(item)
                    settled += len(batch)
                    observe(client, log, -95 + generation * 10,
                            f'老端结清第 {generation + 1} 代 {len(batch)} 条')
                client.set_clock(WINDOW_START[name] + OLD_CLAIM_AT)
                claimed = client.claim(30)['items']
                claimed_by_id = {item['delivery_id']: item for item in claimed}
                observe(client, log, OLD_CLAIM_AT, f'老端最后一次领取 {len(claimed)} 条队首')
                facts['settled_before_cut'] = settled
                facts['in_flight'] = len(claimed)
                facts['lease_expires'] = sorted({item['lease']['expires'] for item in claimed})
                facts['remaining_lease_at_cut'] = facts['lease_expires'][0] - WINDOW_START[name]
                waiting, _ = client.counts()
                facts['waiting_at_cut'] = waiting.get('blocked', 0) + waiting.get('delayed', 0)
                facts['consumer_at_cut'] = waiting

                client.set_clock(WINDOW_START[name] + CUT_AT)
                client.pause(True)
                observe(client, log, CUT_AT, '暂停 consumer，新端不领取')
                probe = client.claim(30, worker='edge-new')
                facts['paused_claim'] = {'paused': probe.get('paused'), 'items': len(probe['items'])}
                # Publishing must keep fanning out while the group is paused.
                fanout = client.publish(f'{name}-during-pause', 'lane-01')['fanout']
                facts['publish_while_paused_fanout'] = fanout
                facts['publish_while_paused_consumer_delivery'] = 'consumer' in fanout
                observe(client, log, CUT_AT, '暂停中发布 1 条（仍生成 consumer 分发）')

                horizon = facts['remaining_lease_at_cut']
                accepted = 0
                leftover, leftover_at = [], None
                if variant == 'paced':
                    cursor = 0
                    for offset, slice_size in ACK_CADENCE[name]:
                        slice_items = claimed[cursor:cursor + slice_size]
                        cursor += slice_size
                        if offset > horizon:
                            # The old fleet only gets to these after the deadline.
                            leftover, leftover_at = slice_items, offset
                            continue
                        client.set_clock(WINDOW_START[name] + offset)
                        for item in slice_items:
                            client.ack(item)
                            accepted += 1
                        observe(client, log, offset,
                                f'暂停期间老端确认 {slice_size} 条（累计成功 {accepted}）')
                facts['old_acks_accepted'] = accepted
                facts['old_acks_deferred_past_deadline'] = len(leftover)

                client.set_clock(WINDOW_START[name] + horizon)
                observe(client, log, horizon, '租约原截止时刻')
                recovered = client.sweep(100)['recovered']
                facts['expired_to_dead'] = [row['delivery_id'] for row in recovered]
                facts['expired_to_dead_count'] = len(recovered)
                observe(client, log, horizon, '全局回收，到期租约转死信', recovered=len(recovered))

                # Old-fleet acks that arrive after the deadline: the late tail in
                # the paced variant, and only leases the sweep could not reach in
                # the freeze variant (already-settled ones stay repeatable).
                handled = {item['delivery_id'] for item in claimed[:accepted]}
                stragglers = [item for item in (leftover or claimed)
                              if item['delivery_id'] not in handled]
                client.set_clock(WINDOW_START[name] + (leftover_at or horizon + 5))
                straggler_status = []
                for item in stragglers:
                    try:
                        client.ack(item)
                        straggler_status.append(200)
                    except RuntimeError:
                        straggler_status.append(409)
                facts['straggler_ack_status_counts'] = {
                    'accepted': straggler_status.count(200),
                    'conflict': straggler_status.count(409)}
                facts['straggler_deliveries'] = [item['delivery_id'] for item in stragglers]
                if stragglers:
                    observe(client, log, leftover_at or horizon + 5,
                            f'老端迟到确认 {len(stragglers)} 个：'
                            f"{straggler_status.count(200)} 接受 / "
                            f"{straggler_status.count(409)} 拒绝")

                if recovered:
                    sample = client.dead()[0]
                    facts['dead_sample'] = {'delivery_id': sample['id'], 'lane': sample['lane'],
                                            'reason': sample['last_attempt']['reason']}
                    raw = client.evidence(sample['message_id']).decode('utf-8')
                    record = json.loads(raw)
                    facts['dead_evidence_sha256'] = record['sha256']
                    holder = claimed_by_id.get(sample['id'])
                    token = holder and holder['lease']['token']
                    facts['dead_evidence_redacts_token'] = bool(token) and token not in raw
                    revoked = next((claimed_by_id[key] for key in facts['expired_to_dead']
                                    if key in claimed_by_id), None)
                    if revoked:
                        try:
                            client.ack(revoked)
                            facts['expired_ack_status'] = 200
                        except RuntimeError:
                            facts['expired_ack_status'] = 409

                resume_at = max(horizon + 8, (leftover_at or 0) + 2)
                client.set_clock(WINDOW_START[name] + resume_at)
                client.pause(False)
                observe(client, log, resume_at, '恢复 consumer，新端开始排空')
                batches, drained = client.drain('consumer', 'edge-new', step=5)
                facts['drain_batches'] = batches
                facts['drained'] = drained
                facts['claim_batch_cap'] = 30
                facts['drain_seconds_assumed'] = batches * 5
                observe(client, log, resume_at + batches * 5,
                        f'新端 {batches} 批排空 {drained} 条')

                arrived = 0
                for offset, count in ARRIVALS[name]:
                    client.set_clock(WINDOW_START[name] + offset)
                    for index in range(count):
                        lane = f'lane-{(index % 12) + 1:02d}'
                        client.publish(f'{name}-arrival-{offset}-{index}', lane,
                                       tags={'feed': 'telemetry' if index % 2 else 'samples'})
                        arrived += 1
                    observe(client, log, offset, f'窗口内新增 {count} 条')
                    pending = client.claim(30, worker='edge-new')['items']
                    for item in pending:
                        client.ack(item)
                    observe(client, log, offset, f'新端当场确认新增 {len(pending)} 条')
                facts['arrivals_in_window'] = arrived

                client.set_clock(WINDOW_START[name] + WINDOW_END)
                final = observe(client, log, WINDOW_END, '关窗')
                facts['observed_at_close'] = final['observed_at']
                facts['consumer_final'] = final['counts']
                facts['drift_final'] = final['drift_counts']
                facts['dead_at_close'] = final['dead']
                facts['dead_ids'] = [row['id'] for row in client.dead()]

                # Same-time retention comparison: with drift-keeper still paused,
                # its waiting deliveries pin their source messages. Drain it
                # without advancing the clock, then re-read the plan.
                client.set_clock(WINDOW_START[name] + WINDOW_END + POLICY['retention_seconds'])
                facts['retention_while_drift_paused'] = len(client.retention()['eligible'])
                drift_before = client.counts(DRIFT_GROUP)[0]
                client.pause(False, group=DRIFT_GROUP)
                drift_batches, drift_settled = client.drain(DRIFT_GROUP, 'drift-new', step=0)
                facts['drift_before_drain'] = drift_before
                facts['drift_drain_batches'] = drift_batches
                facts['drift_drained'] = drift_settled
                facts['retention_after_drift_drain'] = len(client.retention()['eligible'])
                facts['messages_published_total'] = (facts['published_before_cut'] + 1
                                                     + facts['arrivals_in_window'])
                facts['source_messages_pinned_by_sibling'] = (
                    facts['retention_after_drift_drain'] - facts['retention_while_drift_paused'])
                facts['sibling_waiting_at_close'] = (
                    facts['drift_final'].get('blocked', 0) + facts['drift_final'].get('delayed', 0)
                    + facts['drift_final'].get('paused', 0) + facts['drift_final'].get('ready', 0))
                observe(client, log, WINDOW_END + POLICY['retention_seconds'],
                        f'复核：drift-keeper 排空 {drift_settled} 条后解冻保留')
                facts['go_no_go'] = go_no_go(facts, name, variant)
                log.sort(key=lambda row: row['at'])
            finally:
                server.shutdown()
                thread.join()
    return {'facts': facts, 'log': log}


def go_no_go(facts, name, variant):
    """Decision evidence computed from measurements, not from the ops notes."""
    if variant == 'freeze':
        verdict = 'HOLD (freeze strands every in-flight lease)'
    elif facts['expired_to_dead_count'] == 0:
        verdict = 'GO (paused drain, zero dead)'
    else:
        verdict = f"GO with handover list ({facts['expired_to_dead_count']} dead)"
    return {
        'in_flight_at_cut': facts['in_flight'],
        'freeze_cost_dead_letters': facts['in_flight'],
        'paced_cost_dead_letters': facts['expired_to_dead_count'],
        'paced_old_acks_settled': facts['old_acks_accepted'],
        'paced_old_acks_deferred_past_deadline': facts['old_acks_deferred_past_deadline'],
        'keys_waiting_at_cut': facts['waiting_at_cut'],
        'new_fleet_batches': facts['drain_batches'],
        'new_fleet_drain_seconds_assumed': facts['drain_seconds_assumed'],
        'arrivals_in_window': facts['arrivals_in_window'],
        'residual_blocked_in_sibling': facts['drift_final'].get('blocked', 0),
        'residual_paused_in_sibling': facts['drift_final'].get('paused', 0),
        'retention_pinned_by_sibling': facts['retention_while_drift_paused'],
        'sibling_waiting_at_close': facts['sibling_waiting_at_close'],
        'source_messages_pinned_by_sibling': facts['source_messages_pinned_by_sibling'],
        'verdict': verdict,
        'variant': variant,
        'window': name,
    }


def _server(prefix, channel, clock):
    directory = TemporaryDirectory(prefix=prefix)
    persistence = RelayPersistence(Path(directory.name) / 'relay.sqlite3')
    server = QuietServer(('127.0.0.1', 0), persistence, lambda: clock[0])
    thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
    thread.start()
    client = Client(server, clock, channel)
    return directory, server, thread, client


def lease_extension_check():
    """A policy revision cannot lengthen leases already handed out."""
    clock = [WINDOW_START['early'] - 300]
    directory, server, thread, client = _server('quaybell-lease-', 'drill-lease-check', clock)
    try:
        client.post('/channels', {'name': 'drill-lease-check', 'policy': POLICY}, 201)
        client.post('/channels/drill-lease-check/groups', {'name': 'consumer', 'tags': {}}, 201)
        client.publish('old', 'lane-01')
        old = client.claim(1)['items'][0]
        client.post('/channels/drill-lease-check/policy',
                    dict(POLICY, lease_seconds=300, expected=1), 201)
        client.publish('new', 'lane-02')
        new = client.claim(1)['items'][0]
        return {'old_policy': old['policy'],
                'old_lease_seconds': old['lease']['expires'] - clock[0],
                'new_policy': new['policy'],
                'new_lease_seconds': new['lease']['expires'] - clock[0]}
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
        directory.cleanup()


def rollback_check():
    """Pause only blocks new claims; the old fleet can take over again."""
    clock = [WINDOW_START['early'] - 300]
    directory, server, thread, client = _server('quaybell-rollback-', 'drill-rollback', clock)
    try:
        client.post('/channels', {'name': 'drill-rollback', 'policy': POLICY}, 201)
        client.post('/channels/drill-rollback/groups', {'name': 'consumer', 'tags': {}}, 201)
        client.publish('first', 'lane-01')
        client.publish('second', 'lane-02')
        held = client.claim(1)['items'][0]
        client.pause(True)
        paused = client.claim(1, worker='edge-old')
        settled_while_paused = client.ack(held)
        client.pause(False)
        resumed = client.claim(1, worker='edge-old')['items'][0]
        client.ack(resumed)
        trace = client.trace(resumed['message_id'])
        worker = trace['deliveries'][0]['attempts'][0]['worker']
        return {'paused_claim_items': len(paused['items']),
                'ack_while_paused_state': settled_while_paused['state'],
                'resumed_by': worker,
                'resumed_delivery': resumed['delivery_id']}
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
        directory.cleanup()


def bounded_sweep_check():
    """Recovery honours its limit and orders by the original lease deadline."""
    clock = [WINDOW_START['early'] - 300]
    directory, server, thread, client = _server('quaybell-sweep-', 'drill-sweep', clock)
    try:
        client.post('/channels', {'name': 'drill-sweep', 'policy': POLICY}, 201)
        client.post('/channels/drill-sweep/groups', {'name': 'consumer', 'tags': {}}, 201)
        client.publish('first', 'lane-01')
        client.set_clock(clock[0] + 5)
        client.publish('second', 'lane-02')
        first = client.claim(1)['items'][0]
        client.set_clock(clock[0] + 5)
        second = client.claim(1)['items'][0]
        client.set_clock(clock[0] + LEASE + 30)
        one = client.sweep(1)['recovered']
        two = client.sweep(1)['recovered']
        counts, _ = client.counts()
        return {'first_deadline': first['lease']['expires'],
                'second_deadline': second['lease']['expires'],
                'sweep1': [row['delivery_id'] for row in one],
                'sweep1_effective_at': one[0]['effective_at'] if one else None,
                'sweep2': [row['delivery_id'] for row in two],
                'counts_after_two_single_limited_sweeps': counts}
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
        directory.cleanup()


def record_markdown(payload):
    lines = ['# 两段切换窗口演练记录（自动生成）', '',
             f"- 演练锚定：{payload['anchor_date']}（+08:00），受控时钟，单机回环 HTTP，"
             f"`python3 -I docs/window_rehearsal.py` 可重放",
             f"- 策略：`{json.dumps(payload['policy'], ensure_ascii=False, sort_keys=True)}`",
             f"- 在途租约延长检查：{json.dumps(payload['lease_extension_check'], ensure_ascii=False)}",
             f"- 回滚检查：{json.dumps(payload['rollback_check'], ensure_ascii=False)}",
             f"- 有界回收检查：{json.dumps(payload['bounded_sweep_check'], ensure_ascii=False)}",
             '- 旧端最后一次取件：窗口前 30 s；租约统一在窗口后 60 s 到期。',
             '- 说明：`board.messages` 仅保留最近 30 条，消息总量以发布计数为准；'
             '死信计数取 `/dead`，上限 50。',
             '']
    lines += ['## 对照表', '',
              '| 窗口 | 方式 | 窗口前发布 | 已结清 | 在途租约 | 暂停中老端确认 | 到期转死信 | '
              '关窗时等待 | 窗口内新增 | 新端排空 | 批次 | 关窗死信 | 兄弟组残留 | 判定 |',
              '| ' + ' | '.join(['---'] * 14) + ' |']
    for result in payload['results'].values():
        f = result['facts']
        g = f['go_no_go']
        lines.append(f"| {f['label']} | {f['variant']} | {f['published_before_cut']} | "
                     f"{f.get('settled_before_cut', 0)} | {f['in_flight']} | "
                     f"{f['old_acks_accepted']} | {f['expired_to_dead_count']} | "
                     f"{f['waiting_at_cut']} | {f['arrivals_in_window']} | {f['drained']} | "
                     f"{f['drain_batches']} | {f['dead_at_close']} | "
                     f"{f['drift_final'].get('blocked', 0)} 阻挡 / "
                     f"{f['drift_final'].get('paused', 0)} 暂停 | {g['verdict']} |")
    lines.append('')
    for result in payload['results'].values():
        f = result['facts']
        lines += [f"## {f['label']} · {f['variant']}", '',
                  f"- 暂停中取件：{f['paused_claim']}；暂停中发布仍分发：{f['publish_while_paused_fanout']}",
                  f"- 过期凭据确认：{f.get('expired_ack_status', '未触发')}；"
                  f"死信样例：{f.get('dead_sample', '无')}",
                  f"- 证据摘要：{f.get('dead_evidence_sha256', '无')}；"
                  f"凭据不出现在导出：{f.get('dead_evidence_redacts_token', 'n/a')}",
                  f"- 关窗快照 observed_at={f['observed_at_close']}；死信 {f['dead_at_close']} 条 "
                  f"（{f['dead_ids']}）",
                  f"- 同一时刻保留对照：兄弟组仍有 {f['sibling_waiting_at_close']} 条未完成投递，"
                  f"当时保留候选 {f['retention_while_drift_paused']} 条；排空 {f['drift_drained']} 条后"
                  f"解冻 {f['source_messages_pinned_by_sibling']} 条源消息，"
                  f"候选变为 {f['retention_after_drift_drain']}/{f['messages_published_total']}",
                  f"- 判定：{json.dumps(f['go_no_go'], ensure_ascii=False)}",
                  '']
        lines += ['| 偏移(s) | 动作 | consumer 计数 | drift-keeper 计数 | 死信 |',
                  '| --- | --- | --- | --- | --- |']
        for row in result['log']:
            lines.append(f"| {row['offset']:+.0f} | {row['action']} | "
                         f"{json.dumps(row['counts'], ensure_ascii=False)} | "
                         f"{json.dumps(row['drift_counts'], ensure_ascii=False)} | {row['dead']} |")
        lines.append('')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='在两个候选切换窗口上演练本机中继')
    parser.add_argument('window', nargs='?', choices=['early', 'heavy'])
    parser.add_argument('--json', type=Path, default=None)
    parser.add_argument('--markdown', type=Path, default=None)
    args = parser.parse_args()
    names = [args.window] if args.window else ['early', 'heavy']
    results = {}
    for name in names:
        for variant in ('freeze', 'paced'):
            results[f'{name}-{variant}'] = run_window(name, variant)
    payload = {'anchor_date': '2026-11-30', 'timezone': '+08:00', 'policy': POLICY,
               'lease_extension_check': lease_extension_check(),
               'rollback_check': rollback_check(),
               'bounded_sweep_check': bounded_sweep_check(), 'results': results}
    target = args.json or (Path(__file__).parent / 'rehearsal-facts.json')
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')
    if args.markdown:
        args.markdown.write_text(record_markdown(payload), encoding='utf-8')
    for key, result in results.items():
        f = result['facts']
        print(f"{key}: in_flight={f['in_flight']} dead={f['expired_to_dead_count']} "
              f"waiting={f['waiting_at_cut']} drained={f['drained']} "
              f"batches={f['drain_batches']} arrivals={f['arrivals_in_window']} "
              f"dead_at_close={f['dead_at_close']} drift={f['drift_final']} "
              f"verdict={f['go_no_go']['verdict']}")
    print(f'facts -> {target}')


if __name__ == '__main__':
    main()
