"""不可重复设备动作的第三方接入断点示范。

这个模块用一个真实的本机 HTTP Quaybell 实例（临时 SQLite、受控时钟、随机端口）
演示同一条故障故事在两类下游上的不同恢复决定：

1. 设备动作已经执行，消费者在拿到 ACK 回执前退出；
2. 重启后可能仍是原租约，也可能租约已轮换到下一次尝试；
3. 对“能用请求号查询是否执行”的下游，与“超时后只知道连接断了”的下游，
   在同样的断点上分别给出：安全重发确认 / 可以补做 / 必须交人核对；
4. 人工核对后发起的 redrive（人工重投）才代表业务上允许补做。

运行：

    python3 -m examples.device_handoff              # 跑全部场景
    python3 -m examples.device_handoff same-lease   # 仍是原租约：安全重发 ACK
    python3 -m examples.device_handoff queryable    # 请求号可查：查询后确认或补做
    python3 -m examples.device_handoff blind        # 盲下游：交人核对后确认
    python3 -m examples.device_handoff redrive      # 人工重投：授权补做 / 仅补齐关闭

设计要点请对照 INTEGRATION.md 阅读。
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

# 同时支持 `python3 -m examples.device_handoff` 与 `python3 examples/device_handoff.py`。
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from examples.session import session  # type: ignore
else:
    from .session import session

from quaybell.server import RelayRequest

# 本示范使用独立频道/消费组名，避免与 walkthrough 的 wire-gate 混淆。
CHANNEL = '/channels/device-gate'
GROUP = CHANNEL + '/groups/actuators'

# 演示策略：租约 10 秒，首次退避 2 秒，最多 2 次尝试。
POLICY = dict(lease_seconds=10, retry_seconds=2, max_attempts=2, retention_seconds=3600)


# --------------------------------------------------------------------------- #
# 输出辅助
# --------------------------------------------------------------------------- #

def scene(title):
    print('\n' + '=' * 86)
    print(title)
    print('=' * 86)


def step(text):
    print(f'  · {text}')


def decision(tag, text):
    colors = {
        'ack': '[可安全重发确认]',
        'redo': '[可以补做]      ',
        'human': '[必须交人核对]  ',
        'stop': '[禁止自动补做]  ',
        'fact': '[中继事实]      ',
    }
    print(f'  {colors[tag]} {text}')


def result_line(text):
    print(f'  ✓ {text}')


# --------------------------------------------------------------------------- #
# 最小 HTTP 客户端（第三方接入方照此实现即可，仅用标准库）
# --------------------------------------------------------------------------- #

class Relay:
    def __init__(self, base):
        self.base = base

    def call(self, method, path, payload=None):
        kwargs = {}
        if payload is not None:
            kwargs['data'] = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            kwargs['headers'] = {'Content-Type': 'application/json'}
        req = Request(self.base + path, method=method, **kwargs)
        try:
            with urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode('utf-8'))
        except HTTPError as exc:
            return exc.code, json.loads(exc.read().decode('utf-8'))

    def get(self, path):
        return self.call('GET', path)

    def post(self, path, payload):
        return self.call('POST', path, payload)

    def expect(self, method, path, payload=None, code=200):
        actual, body = self.call(method, path, payload)
        assert actual == code, (method, path, actual, code, body)
        return body


def setup_channel(client, policy=POLICY):
    relay = Relay(client.base)
    relay.expect('POST', '/channels', {'name': 'device-gate', 'policy': policy}, 201)
    relay.expect('POST', CHANNEL + '/groups', {'name': 'actuators', 'tags': {}}, 201)
    return relay


def publish_command(relay, dedup_key, lane, command_id):
    return relay.expect('POST', CHANNEL + '/messages', {
        'producer': 'device-bridge',
        'key': dedup_key,
        'lane': lane,
        'tags': {},
        'payload': {
            'command_id': command_id,
            'device': 'valve-7',
            'op': 'close',
            # request_no 是接入方为下游副作用选择的稳定请求号。
            'request_no': 'REQ-' + command_id,
        },
    }, code=201)


def claim_one(relay, worker='bridge-worker'):
    body = relay.expect('POST', GROUP + '/claim', {'worker': worker, 'limit': 1})
    assert len(body['items']) == 1, body
    return body['items'][0]


# --------------------------------------------------------------------------- #
# 接入方自己的持久台账：崩溃/重启后唯一可信的本地状态
# 真实系统应放在与业务写入同一事务的数据库里；这里用原子替换的 JSON 文件。
# --------------------------------------------------------------------------- #

class Ledger:
    def __init__(self, directory):
        self.path = Path(directory) / 'handoff-ledger.json'
        self.rows = json.loads(self.path.read_text('utf-8')) if self.path.exists() else {}

    def get(self, delivery_id):
        return self.rows.get(str(delivery_id))

    def put(self, delivery_id, **changes):
        row = dict(self.rows.get(str(delivery_id), {}))
        row.update(changes)
        row['delivery_id'] = int(delivery_id)
        self.rows[str(delivery_id)] = row
        tmp = self.path.with_name(self.path.name + '.tmp')
        tmp.write_text(json.dumps(self.rows, ensure_ascii=False, indent=1), 'utf-8')
        os.replace(tmp, self.path)  # 原子替换，保证台账文件不会写一半


class ProcessExit(Exception):
    """模拟消费者进程在指定断点被杀：内存对象全部丢弃，只能从磁盘台账重启。"""


def crash(reason):
    raise ProcessExit(reason)


class ApprovalRequired(Exception):
    pass


# --------------------------------------------------------------------------- #
# 两类下游设备
# --------------------------------------------------------------------------- #

class AmbiguousOutcome(Exception):
    """超时/连接断开：调用方无法判断设备是否已经执行。两类盲调用看到的错误完全一样。"""


class QueryableDevice:
    """第一类下游：支持按请求号查询执行状态，且同请求号重复调用幂等。"""

    kind = 'queryable'

    def __init__(self):
        self.by_request = {}   # request_no -> 'executed'
        self.physical_actuations = 0
        self.execute_calls = 0

    def execute(self, request_no):
        self.execute_calls += 1
        if request_no in self.by_request:
            return 'duplicate_skipped'       # 下游按请求号去重，不会二次动作
        self.by_request[request_no] = 'executed'
        self.physical_actuations += 1
        return 'accepted'

    def query(self, request_no):
        return self.by_request.get(request_no)  # 'executed' 或 None（无记录）

    def timing_out_call(self, request_no, hidden_truth):
        """模拟第一次调用超时。hidden_truth 只代表物理世界真相，调用方查不到。"""
        if hidden_truth == 'executed':
            self.by_request[request_no] = 'executed'
            self.physical_actuations += 1
        raise AmbiguousOutcome('读响应超时：连接已断开，不知道服务端是否执行')


class BlindDevice:
    """第二类下游：没有查询接口，超时后调用方只知道连接断了。"""

    kind = 'blind'

    def __init__(self):
        self.physical_actuations = 0

    def execute(self, request_no):
        # 人工授权补做后的新请求号走到这里，设备真实动作一次。
        self.physical_actuations += 1
        return 'accepted'

    def call_and_lose_connection(self, hidden_truth):
        # 无论 hidden_truth 是哪种，抛出的错误都相同，worker 无法据此判断。
        if hidden_truth == 'executed':
            self.physical_actuations += 1
        raise AmbiguousOutcome('写请求后连接中断，无响应、无查询入口')


# --------------------------------------------------------------------------- #
# 人工核对/审批工单（演示里用持久 JSON 模拟运维工单系统）
# --------------------------------------------------------------------------- #

class HumanDesk:
    def __init__(self, directory):
        self.path = Path(directory) / 'human-tickets.json'
        self.tickets = json.loads(self.path.read_text('utf-8')) if self.path.exists() else {}

    def _save(self):
        tmp = self.path.with_name(self.path.name + '.tmp')
        tmp.write_text(json.dumps(self.tickets, ensure_ascii=False, indent=1), 'utf-8')
        os.replace(tmp, self.path)

    def open_check(self, dead_or_delivery_id, message_id, request_no, reason):
        self.tickets[str(dead_or_delivery_id)] = {
            'delivery_id': dead_or_delivery_id, 'message_id': message_id,
            'request_no': request_no, 'reason': reason, 'state': 'open'}
        self._save()

    def record_finding(self, delivery_id, finding, approver):
        ticket = self.tickets[str(delivery_id)]
        ticket.update(state='resolved', finding=finding, approver=approver)
        self._save()

    def grant_redo(self, delivery_id, approver):
        self.record_finding(delivery_id, 'not_executed', approver)
        self.tickets[str(delivery_id)]['redo_authorized'] = True
        self._save()

    def redo_approval(self, dead_delivery_id):
        ticket = self.tickets.get(str(dead_delivery_id))
        if ticket and ticket.get('redo_authorized') is True:
            return ticket
        return None

    def close_only_verdict(self, dead_delivery_id):
        ticket = self.tickets.get(str(dead_delivery_id))
        if ticket and ticket.get('finding') == 'executed':
            return ticket
        return None


# --------------------------------------------------------------------------- #
# 参考接入 Worker：所有恢复决定都在这里，指南的决策表与之一一对应
# --------------------------------------------------------------------------- #

class HandoffWorker:
    def __init__(self, relay, ledger_dir, device, desk):
        self.relay = relay
        self.ledger = Ledger(ledger_dir)   # 重启 = 新对象重新读盘，不继承任何内存
        self.device = device
        self.desk = desk

    # -- 领取后立刻持久化凭据与请求号 ------------------------------------- #
    def remember(self, item):
        did = item['delivery_id']
        payload = item['envelope']['payload']
        row = self.ledger.get(did)
        if row is None:
            self.ledger.put(did, message_id=item['message_id'], attempt=item['lease']['number'],
                            token=item['lease']['token'], request_no=payload['request_no'],
                            phase='claimed')
        elif row.get('token') != item['lease']['token']:
            # 同一 delivery 的下一次尝试：保留旧凭据用于“重发确认”探测，登记新凭据。
            self.ledger.put(did, previous_token=row['token'], previous_attempt=row['attempt'],
                            attempt=item['lease']['number'], token=item['lease']['token'])
        return self.ledger.get(did)

    # -- 断点 1/2 之后的第一个安全动作：用台账里的原凭据重发 ACK ----------- #
    def replay_ack_with_stored_token(self, did):
        row = self.ledger.get(did)
        code, body = self.relay.call('POST', f'/deliveries/{did}/ack', {'token': row['token']})
        if code == 200:
            self.ledger.put(did, phase='ack_confirmed', ack=body)
            return 'repeated' if body.get('repeated') else 'recorded'
        if code == 409:
            return 'conflict'
        raise AssertionError((code, body))

    def ack_current(self, item):
        did = item['delivery_id']
        token = self.ledger.get(did)['token']
        body = self.relay.expect('POST', f'/deliveries/{did}/ack', {'token': token})
        self.ledger.put(did, phase='ack_confirmed', ack=body)
        return body

    def nack_current(self, item, reason):
        token = self.ledger.get(item['delivery_id'])['token']
        return self.relay.expect('POST', f"/deliveries/{item['delivery_id']}/nack",
                                 {'token': token, 'reason': reason})

    # -- 租约轮换后的对账：按下游能力分流 ---------------------------------- #
    def reconcile_rotated_attempt(self, item):
        did = item['delivery_id']
        row = self.remember(item)
        request_no = row['request_no']

        if isinstance(self.device, QueryableDevice):
            state = self.device.query(request_no)
            if state == 'executed':
                decision('ack', f'请求号 {request_no} 查询结果=已执行：不再调用设备，'
                                f'用第 {item["lease"]["number"]} 次尝试的新凭据 ACK')
                return 'acked_without_recall', self.ack_current(item)
            if state is None:
                decision('redo', f'请求号 {request_no} 查询无记录：说明首次未生效，'
                                 f'用同一个请求号补做（下游幂等），随后 ACK')
                outcome = self.device.execute(request_no)
                assert outcome in ('accepted', 'duplicate_skipped')
                self.ledger.put(did, phase='device_done')
                return 'redone_same_request_no', self.ack_current(item)
            raise AssertionError(('unexpected query state', state))

        # 盲下游：没有 query()，任何“再调一次”都可能造成第二次物理动作。
        self.desk.open_check(did, item['message_id'], request_no,
                             '下游超时且无查询入口，租约已轮换，自动补做可能重复动作')
        decision('human', f'{request_no} 超时后只知道连接断了，无法判断设备是否动作；'
                          f'已开工单并停止自动补做，等待人工核对 delivery={did}')
        return 'needs_human', None


def send_ack_but_lose_receipt(base, relay, item):
    """真实制造“中继已提交 ACK，但消费者没拿到回执”：

    用裸 socket 发出完整 ACK 请求但不读取响应；另开只读查询确认中继已经记录 acked，
    然后关闭这个从没读过响应的连接。对端状态与“回执在网络中丢失”完全一致。
    """
    parsed = urlparse(base)
    body = json.dumps({'token': item['lease']['token']}).encode('utf-8')
    head = (f"POST /v1/deliveries/{item['delivery_id']}/ack HTTP/1.1\r\n"
            f'Host: {parsed.hostname}:{parsed.port}\r\n'
            'Content-Type: application/json\r\n'
            f'Content-Length: {len(body)}\r\nConnection: close\r\n\r\n').encode('ascii')
    sock = socket.create_connection((parsed.hostname, parsed.port), timeout=3)
    try:
        sock.sendall(head + body)
        for _ in range(100):
            trace = relay.expect('GET', f"/messages/{item['message_id']}")
            if any(event['kind'] == 'acked' for event in trace['events']):
                return
            time.sleep(0.02)
        raise AssertionError('中继没有在限定时间内记录 ACK')
    finally:
        sock.close()


def operator_redrive(relay, desk, dead_delivery_id):
    """运维侧人工重投入口：必须能出示 redo_authorized 审批，否则接入方脚本拒绝执行。"""
    if desk.redo_approval(dead_delivery_id) is None:
        raise ApprovalRequired(f'dead delivery {dead_delivery_id} 没有“核对未执行+授权补做”工单')
    return relay.expect('POST', f'/deliveries/{dead_delivery_id}/redrive', {}, 201)


def trace_delivery(relay, message_id, delivery_id):
    trace = relay.expect('GET', f'/messages/{message_id}')
    return next(row for row in trace['deliveries'] if row['id'] == delivery_id), trace


# --------------------------------------------------------------------------- #
# 场景一：重启后仍是原租约 —— 安全重发 ACK
# --------------------------------------------------------------------------- #

def scenario_same_lease():
    scene('场景 1｜设备已执行，ACK 回执前退出；重启后仍是原租约')
    with session() as clock_session, tempfile.TemporaryDirectory(prefix='handoff-') as ledger_dir:
        relay = setup_channel(clock_session)
        desk = HumanDesk(ledger_dir)

        # --- 消息 A：ACK 根本没发出去就崩溃 ------------------------------- #
        # 这里刻意使用“无查询接口”的盲下游且调用明确成功：只要成功事实已持久化，
        # 原租约内重发 ACK 的安全性与下游类型无关。
        publish_command(relay, 'cmd-ack-unsent', 'lane-ack-a', 'ACK-UNSENT')
        item = claim_one(relay)
        device_a = BlindDevice()
        worker = HandoffWorker(relay, ledger_dir, device_a, desk)
        row = worker.remember(item)
        worker.device.execute(row['request_no'])
        worker.ledger.put(item['delivery_id'], phase='device_done')
        step('盲下游明确返回成功，设备已动作 1 次，台账写入 device_done；ACK 发送前进程被杀')
        try:
            crash('SIGKILL before ack send')
        except ProcessExit as gone:
            step(f'旧进程退出（{gone}），内存状态全部丢失')

        # 真实“重启”：丢弃 worker，仅靠同一磁盘台账构造新对象。
        worker = HandoffWorker(relay, ledger_dir, device_a, desk)
        outcome = worker.replay_ack_with_stored_token(item['delivery_id'])
        assert outcome == 'recorded'
        decision('ack', f'原租约未到期，台账里的原 token 重发 ACK → 200（首次记录，repeated=false）')
        result_line('设备动作次数仍为 1，没有再次调用设备；此决定不依赖下游是否可查询')
        assert device_a.physical_actuations == 1

        # --- 消息 B：ACK 已被中继记录，但回执丢失 -------------------------- #
        publish_command(relay, 'cmd-ack-lost', 'lane-ack-b', 'ACK-LOST')
        item_b = claim_one(relay)
        device_b = QueryableDevice()
        worker_b = HandoffWorker(relay, ledger_dir, device_b, desk)
        row_b = worker_b.remember(item_b)
        device_b.execute(row_b['request_no'])
        worker_b.ledger.put(item_b['delivery_id'], phase='device_done')
        send_ack_but_lose_receipt(clock_session.base, relay, item_b)
        step('ACK 请求已到达中继并提交，但消费者的响应从未被读取（等效回执丢失）')
        try:
            crash('SIGKILL before ack receipt persisted')
        except ProcessExit as gone:
            step(f'旧进程退出（{gone}）；中继有 acked 记录，新进程手里没有回执')

        worker_b = HandoffWorker(relay, ledger_dir, device_b, desk)
        outcome = worker_b.replay_ack_with_stored_token(item_b['delivery_id'])
        assert outcome == 'repeated'
        decision('ack', '重启后用同一 token 重发 → 200 repeated=true，读到中继已保存的同一回执')

        clock_session.tick(9_999)  # 即使远超租约到期时刻
        outcome = worker_b.replay_ack_with_stored_token(item_b['delivery_id'])
        assert outcome == 'repeated'
        decision('fact', 'ACK 一旦已记录，重放读取回执先于租约超时判断；很久以后仍是 repeated=true')
        assert device_b.physical_actuations == 1
        result_line('两条命令各物理动作 1 次；重发的只是确认，不是设备指令')
        return {'unsent': 'recorded', 'receipt_lost': 'repeated', 'device_actuations': 2}


# --------------------------------------------------------------------------- #
# 场景二：租约轮换到下一次尝试 —— 可查询下游
# --------------------------------------------------------------------------- #

def scenario_queryable():
    scene('场景 2｜同样的断点，请求号可查的下游：查询决定“确认”还是“同号补做”')
    with session() as clock_session, tempfile.TemporaryDirectory(prefix='handoff-') as ledger_dir:
        relay = setup_channel(clock_session)
        desk = HumanDesk(ledger_dir)
        device = QueryableDevice()

        # --- 2a 首次确实执行了，但结果还没来得及落台账就崩溃 --------------- #
        publish_command(relay, 'cmd-q-done', 'lane-q-a', 'Q-DONE')
        first = claim_one(relay)
        worker = HandoffWorker(relay, ledger_dir, device, desk)
        row = worker.remember(first)
        try:
            device.timing_out_call(row['request_no'], hidden_truth='executed')
        except AmbiguousOutcome:
            worker.ledger.put(first['delivery_id'], phase='in_flight')  # 只有请求号，没有结论
        step('请求号 Q-DONE 已在下游执行，但调用超时、本地台账停在 in_flight')
        try:
            crash('SIGKILL after ambiguous call')
        except ProcessExit:
            step('消费者退出；13 秒后才被值班进程重新拉起（租约已过期）')

        clock_session.tick(13)  # 超过 10 秒租约 + 2 秒退避
        worker = HandoffWorker(relay, ledger_dir, device, desk)
        assert worker.replay_ack_with_stored_token(first['delivery_id']) == 'conflict'
        decision('fact', f'重启后用第 1 次尝试的旧 token 补 ACK → 409，租约已超时轮换（delivery={first["delivery_id"]}）')

        second = claim_one(relay)
        assert second['lease']['number'] == 2 and second['lease']['token'] != first['lease']['token']
        decision('fact', f'重新领取得到第 2 次尝试和新 token（attempt=2, delivery={second["delivery_id"]}）')
        tag, ack_body = worker.reconcile_rotated_attempt(second)
        assert tag == 'acked_without_recall' and ack_body['state'] == 'acked'

        # --- 2b 首次根本没生效：查询无记录 → 同请求号补做 ------------------ #
        publish_command(relay, 'cmd-q-missing', 'lane-q-b', 'Q-MISSING')
        first_b = claim_one(relay)
        worker_b = HandoffWorker(relay, ledger_dir, device, desk)
        row_b = worker_b.remember(first_b)
        try:
            device.timing_out_call(row_b['request_no'], hidden_truth='never_arrived')
        except AmbiguousOutcome:
            worker_b.ledger.put(first_b['delivery_id'], phase='in_flight')
        step('Q-MISSING 首次调用超时；物理世界其实未动作（演示隐藏事实，接入方此时并不知道）')
        try:
            crash('SIGKILL after timeout')
        except ProcessExit:
            step('消费者带着 in_flight 台账退出，稍后在新租约上重启')

        clock_session.tick(13)
        worker_b = HandoffWorker(relay, ledger_dir, device, desk)
        assert worker_b.replay_ack_with_stored_token(first_b['delivery_id']) == 'conflict'
        second_b = claim_one(relay)
        tag, _ = worker_b.reconcile_rotated_attempt(second_b)
        assert tag == 'redone_same_request_no'

        # 同请求号再调一次也被下游去重，不会产生第二次物理动作。
        assert device.execute(row_b['request_no']) == 'duplicate_skipped'
        result_line(f'物理动作总数={device.physical_actuations}（每条命令恰好 1 次），'
                    f'execute 调用次数={device.execute_calls}（含被去重的重复调用）')
        assert device.physical_actuations == 2
        return {'executed_branch': 'query->ack', 'missing_branch': 'query->same-request-redo'}


# --------------------------------------------------------------------------- #
# 场景三：盲下游超时 —— 必须交人核对；核对“已执行”后确认当前尝试
# --------------------------------------------------------------------------- #

def scenario_blind_human_done():
    scene('场景 3｜同样的断点，超时后只知道连接断了：停止自动补做，交人核对')
    with session() as clock_session, tempfile.TemporaryDirectory(prefix='handoff-') as ledger_dir:
        relay = setup_channel(clock_session)
        desk = HumanDesk(ledger_dir)
        device = BlindDevice()  # 注意：没有 query()

        publish_command(relay, 'cmd-blind-done', 'lane-b', 'BLIND-DONE')
        first = claim_one(relay)
        worker = HandoffWorker(relay, ledger_dir, device, desk)
        row = worker.remember(first)
        try:
            device.call_and_lose_connection(hidden_truth='executed')  # 物理上已动作
        except AmbiguousOutcome:
            worker.ledger.put(first['delivery_id'], phase='unknown')
        step('设备实际已动作（隐藏事实），但调用方只收到连接断开；台账记 unknown')
        try:
            crash('SIGKILL with unknown outcome')
        except ProcessExit:
            step('消费者退出；重启时租约可能仍在，也可能已轮换——本场景演示轮换后的处理')

        clock_session.tick(13)
        worker = HandoffWorker(relay, ledger_dir, device, desk)
        assert worker.replay_ack_with_stored_token(first['delivery_id']) == 'conflict'
        second = claim_one(relay)
        assert second['lease']['number'] == 2
        tag, _ = worker.reconcile_rotated_attempt(second)
        assert tag == 'needs_human'
        assert desk.tickets[str(first['delivery_id'])]['state'] == 'open'
        result_line('Worker 没有再次调用设备，也没有 ACK；中继侧该投递仍由第 2 次尝试持有')

        # 人工到现场/设备维护终端核对（带外事实源），确认阀门确实已关闭。
        assert device.physical_actuations == 1  # 演示里把它当作现场看到的设备状态
        desk.record_finding(first['delivery_id'], 'executed', approver='ops-wang')
        decision('human', '人工现场核对结论=已执行，并在工单系统登记')

        # 受控时钟下人工核对“瞬间”完成，第 2 次租约仍有效；真实环境若已超时，
        # 就重新 claim 当前尝试再 ACK，若已 dead 则走场景 4 的 redrive 后继。
        ack_body = worker.ack_current(second)
        assert ack_body['state'] == 'acked'
        decision('ack', '凭“人工已核对执行”的工单 ACK 第 2 次尝试；全程没有第二次设备调用')
        result_line('再次强调：这个 ACK 只证明中继处置已记录；设备已执行的结论来自人工核对/设备侧凭据')
        assert device.physical_actuations == 1
        return {'blind_timeout': 'needs_human', 'finding': 'executed', 'settled': 'acked_by_second_attempt'}


# --------------------------------------------------------------------------- #
# 场景四：人工重投（redrive）= 明确允许补做；以及“已执行但已死信”的仅关闭路径
# --------------------------------------------------------------------------- #

def scenario_redrive():
    scene('场景 4｜死信后的人工 redrive：一次人工重投代表“有意重新处理/授权补做”')
    with session() as clock_session, tempfile.TemporaryDirectory(prefix='handoff-') as ledger_dir:
        relay = setup_channel(clock_session)
        desk = HumanDesk(ledger_dir)

        # ---- 4a 人工核对“未执行”，授权补做 ------------------------------- #
        step('--- 4a 现场确认设备未动作：人工核对 + 授权补做 + redrive ---')
        device = BlindDevice()
        publish_command(relay, 'cmd-redo', 'lane-r-a', 'REDO-A')
        first = claim_one(relay)
        worker = HandoffWorker(relay, ledger_dir, device, desk)
        row = worker.remember(first)
        try:
            device.call_and_lose_connection(hidden_truth='never_arrived')
        except AmbiguousOutcome:
            worker.ledger.put(first['delivery_id'], phase='unknown')
        try:
            crash('SIGKILL with unknown outcome')
        except ProcessExit:
            step('首次尝试在 unknown 状态下退出')

        clock_session.tick(13)
        worker = HandoffWorker(relay, ledger_dir, device, desk)
        assert worker.replay_ack_with_stored_token(first['delivery_id']) == 'conflict'
        second = claim_one(relay)
        tag, _ = worker.reconcile_rotated_attempt(second)
        assert tag == 'needs_human'
        dead_body = worker.nack_current(
            second, '下游结果不明，禁止自动重试；等待人工核对（最后一次尝试）')
        assert dead_body['state'] == 'dead'
        decision('fact', f'第 2 次（最后一次）尝试 nack 后 delivery={first["delivery_id"]} 进入 dead')

        # 没有补做授权时，运维 redrive 脚本必须拒绝。
        try:
            operator_redrive(relay, desk, first['delivery_id'])
            raise AssertionError('应当因为缺少审批而拒绝')
        except ApprovalRequired:
            decision('stop', '尚无“核对未执行 + 授权补做”工单时，自动化运维入口拒绝 redrive')

        # 人工现场确认阀门仍开着（physical_actuations == 0），签发补做授权。
        assert device.physical_actuations == 0
        desk.grant_redo(first['delivery_id'], approver='ops-li')
        redrive = operator_redrive(relay, desk, first['delivery_id'])
        child_id = redrive['delivery_id']
        assert redrive['repeated'] is False
        decision('redo', f'人工执行 POST /deliveries/{first["delivery_id"]}/redrive → '
                         f'追加后继 delivery={child_id}（新 ID、队尾、当前策略），这就是补做许可')

        again = relay.expect('POST', f'/deliveries/{first["delivery_id"]}/redrive', {}, 200)
        assert again['delivery_id'] == child_id and again['repeated'] is True
        decision('fact', '重复 redrive 请求返回同一个后继 repeated=true，不会凭空追加第二条补做投递')

        child = claim_one(relay)
        assert child['delivery_id'] == child_id and child['lease']['number'] == 1
        child_row, trace = trace_delivery(relay, child['message_id'], child_id)
        assert child_row['parent_id'] == first['delivery_id']
        assert any(event['kind'] == 'redriven' for event in trace['events'])

        # 后继 worker 必须同时核对血缘和人工授权；这里用新 worker + 同一工单目录演示。
        child_worker = HandoffWorker(relay, ledger_dir, device, desk)
        child_worker.remember(child)
        approval = desk.redo_approval(child_row['parent_id'])
        assert approval is not None
        step(f'后继从 trace 确认 parent_id={child_row["parent_id"]} 且存在 redriven 事件，'
             f'并查到审批人 {approval["approver"]} 的 redo_authorized 工单')

        # 盲下游 + 曾断连：补做使用绑定 redrive 世代的新请求号，避免与上次悬而未决的请求混淆；
        # “第一次其实没动作”已由人工现场核对关闭了重复动作风险。
        redo_request_no = f'{row["request_no"]}#redo-{child_id}'
        child_worker.ledger.put(child_id, request_no=redo_request_no, phase='in_flight')
        assert device.execute(redo_request_no) == 'accepted'
        child_worker.ledger.put(child_id, phase='device_done')
        child_worker.ack_current(child)
        result_line(f'授权补做完成：设备物理动作次数={device.physical_actuations}（恰好 1 次），后继已 ACK')
        assert device.physical_actuations == 1

        # ---- 4b 人工核对“已执行”但投递已 dead：redrive 仅用于补齐关闭 ---- #
        step('--- 4b 死信后人工确认其实已执行：redrive 后继只 ACK，不再调用设备 ---')
        device_b = BlindDevice()
        publish_command(relay, 'cmd-already-done', 'lane-r-b', 'REDO-DONE')
        first_b = claim_one(relay)
        worker_b = HandoffWorker(relay, ledger_dir, device_b, desk)
        row_b = worker_b.remember(first_b)
        try:
            device_b.call_and_lose_connection(hidden_truth='executed')
        except AmbiguousOutcome:
            worker_b.ledger.put(first_b['delivery_id'], phase='unknown')
        clock_session.tick(13)
        worker_b = HandoffWorker(relay, ledger_dir, device_b, desk)
        assert worker_b.replay_ack_with_stored_token(first_b['delivery_id']) == 'conflict'
        second_b = claim_one(relay)
        worker_b.reconcile_rotated_attempt(second_b)
        dead_b = worker_b.nack_current(second_b, '等待人工核对（最后一次尝试）')
        assert dead_b['state'] == 'dead'

        desk.record_finding(first_b['delivery_id'], 'executed', approver='ops-zhao')
        # 已执行结论不授予 redo；运维 redrive 仍需一个明确工单，这里登记为 close-only 后执行。
        desk.tickets[str(first_b['delivery_id'])]['close_only'] = True
        desk._save()
        relay.expect('POST', f'/deliveries/{first_b["delivery_id"]}/redrive', {}, 201)
        decision('human', '人工结论=已执行：redrive 在这里只是“核对后补一个后继来关闭死信”，不是再做一次')

        child_b = claim_one(relay)
        wb2 = HandoffWorker(relay, ledger_dir, device_b, desk)
        wb2.remember(child_b)
        verdict = desk.close_only_verdict(first_b['delivery_id'])
        assert verdict and verdict['finding'] == 'executed'
        wb2.ack_current(child_b)  # 不调用 device_b
        result_line(f'close-only 后继直接 ACK；该设备物理动作次数={device_b.physical_actuations}（仍为 1）')
        assert device_b.physical_actuations == 1

        return {'redo_authorized': child_id, 'redo_actuations': 1, 'close_only': 'acked_without_device_call'}


SCENARIOS = {
    'same-lease': scenario_same_lease,
    'queryable': scenario_queryable,
    'blind': scenario_blind_human_done,
    'redrive': scenario_redrive,
}


def print_matrix():
    print('\n' + '-' * 86)
    print('同样断点上的决定矩阵（完整版见 INTEGRATION.md）')
    print('-' * 86)
    rows = [
        ('原 token 重发 ACK → 200',
         '安全：未记录则补记，已记录则 repeated=true；都不要再调设备',
         '相同（前提：本地台账已持久化“设备已完成”）'),
        ('旧 token → 409，领到第 2 次尝试',
         '请求号查询：executed→直接 ACK；无记录→同请求号补做后 ACK',
         '结果不明：禁止自动补做，开工单交人核对'),
        ('人工核对：设备已执行',
         '通常查询已能确认；以设备侧记录为准 ACK 当前尝试',
         '凭人工工单 ACK 当前尝试；已 dead 则 redrive 后 ACK 后继'),
        ('人工核对：设备未执行 + 授权补做',
         '无记录时一般已可同号补做；也可走人工 redrive 世代',
         '人工 redrive 后继才是补做许可；建议用绑定后继的新请求号'),
    ]
    for i, (when, queryable, blind) in enumerate(rows, 1):
        print(f'  {i}. {when}')
        print(f'     ├─ 请求号可查：{queryable}')
        print(f'     └─ 盲下游    ：{blind}')


def main(argv=None):
    # 作为脚本运行时屏蔽 http.server 访问日志，让断点叙述更清晰；import 本模块不产生副作用。
    RelayRequest.log_message = lambda *args, **kwargs: None
    parser = argparse.ArgumentParser(description='Quaybell 不可重复设备动作接入断点示范')
    parser.add_argument('scenario', nargs='?', choices=[*SCENARIOS, 'all'], default='all')
    args = parser.parse_args(argv)

    names = list(SCENARIOS) if args.scenario == 'all' else [args.scenario]
    facts = {}
    for name in names:
        facts[name] = SCENARIOS[name]()
        print(f'\n  [{name}] 断言全部通过：{json.dumps(facts[name], ensure_ascii=False)}')

    if args.scenario == 'all':
        print_matrix()
        print('\n全部 4 个示范场景通过：安全重发 ACK / 可查补做 / 交人核对 / 人工重投均已本机验证。')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
