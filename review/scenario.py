"""可复现的同构样本：一次清理、原键复用、一条迟来的重投回执。

全部走公共 HTTP 接口、受控时钟和临时 SQLite 文件。运行后把每条声明所需的
原始材料写入 ``review/evidence/<run>/``。时钟固定为 2026-11-27T00:00:00Z 起算，
因此同一版代码在任意机器上产出相同的身份、时间戳和指纹；只有租约凭据每次随机，
而凭据从不写入证据。样本只代表构造出来的同构情形，不代表任何生产事实。
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from quaybell.codec import fingerprint, pack
from quaybell.persistence import RelayPersistence
from quaybell.server import RelayServer

FORMAT = 'quaybell-review/1'

# 与告警同构：清理后原键复用、载荷相同。
POLICY = dict(lease_seconds=300, retry_seconds=5, max_attempts=1, retention_seconds=120)
ORIGINAL = dict(producer='sensor-gate', key='alert-7', lane='coil-north',
                tags={'stream': 'alert'}, payload={'reading': 48, 'unit': 'mV'},
                delay_seconds=0)
# 迟来回执里只有父子分发号 1 → 2；待判定的「新消息」是分发 3。
LATE_PARENT, LATE_CHILD, NEW_DELIVERY = 1, 2, 3


class Clock:
    def __init__(self):
        self.base = int(datetime(2026, 11, 27, tzinfo=timezone.utc).timestamp())
        self.value = float(self.base)

    def at(self, hour, minute, second=0):
        self.value = float(self.base + hour * 3600 + minute * 60 + second)
        return self.value


class Rig:
    """只用公开 HTTP 入口的客户端；另存原始字节以便计算内容哈希。"""

    def __init__(self, server):
        self.base = f'http://127.0.0.1:{server.server_port}/v1'

    def raw(self, path, data=None):
        info = {} if data is None else dict(
            data=json.dumps(data, ensure_ascii=False).encode('utf-8'),
            headers={'Content-Type': 'application/json'})
        try:
            response = urlopen(Request(self.base + path, **info), timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            return response.code, response.read()

    def call(self, path, data=None, expected=200):
        code, body = self.raw(path, data)
        parsed = json.loads(body)
        if code != expected:
            raise RuntimeError((path, code, expected, parsed))
        return parsed

    def evidence(self, message_id):
        code, body = self.raw(f'/messages/{message_id}/evidence')
        if code != 200:
            raise RuntimeError((message_id, code, json.loads(body)))
        return body


def run() -> dict:
    clock = Clock()
    directory = TemporaryDirectory(prefix='quaybell-review-')
    persistence = RelayPersistence(Path(directory.name) / 'relay.sqlite3')
    facts, evidence = {}, {}
    server = RelayServer(('127.0.0.1', 0), persistence, lambda: clock.value)
    thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
    thread.start()
    try:
        rig = Rig(server)

        # 05:00 建立两条链路的频道。alert-desk 承载告警本体，alert-mirror 为补充材料。
        clock.at(5, 0)
        rig.call('/channels', {'name': 'alert-desk', 'policy': POLICY}, 201)
        rig.call('/channels/alert-desk/groups', {'name': 'primary-desk',
                                                 'tags': {'stream': 'alert'}}, 201)
        rig.call('/channels', {'name': 'alert-mirror', 'policy': POLICY}, 201)
        rig.call('/channels/alert-mirror/groups', {'name': 'primary-desk',
                                                   'tags': {'stream': 'alert'}}, 201)

        # 05:00:00 旧消息发布；05:00:01 领取；05:00:03 明确拒绝 → 死信（max_attempts=1）。
        clock.at(5, 0, 0)
        facts['original_publish'] = rig.call('/channels/alert-desk/messages', ORIGINAL, 201)
        clock.at(5, 0, 1)
        first = rig.call('/channels/alert-desk/groups/primary-desk/claim',
                         {'worker': 'desk-a', 'limit': 1})['items'][0]
        clock.at(5, 0, 3)
        facts['original_nack'] = rig.call(
            f"/deliveries/{first['delivery_id']}/nack",
            {'token': first['lease']['token'], 'reason': '告警帧校验失败'})
        facts['original_claim'] = dict(delivery_id=first['delivery_id'],
                                       attempt=first['lease']['id'],
                                       expires=first['lease']['expires'])

        # 05:00:04 死信重投 → 子分发 2；05:00:05 领取；05:00:06 确认。旧链在此收尾。
        clock.at(5, 0, 4)
        facts['original_redrive'] = rig.call(f"/deliveries/{first['delivery_id']}/redrive", {}, 201)
        clock.at(5, 0, 5)
        child = rig.call('/channels/alert-desk/groups/primary-desk/claim',
                         {'worker': 'desk-b', 'limit': 1})['items'][0]
        clock.at(5, 0, 6)
        facts['original_child_ack'] = rig.call(
            f"/deliveries/{child['delivery_id']}/ack", {'token': child['lease']['token']})

        clock.at(5, 0, 10)
        evidence['original'] = rig.evidence(facts['original_publish']['message_id'])

        # 05:02:30 复核并清理旧消息（保留 120 秒、05:00:00 发布）。原键随之可复用。
        clock.at(5, 2, 30)
        facts['original_retention_plan'] = rig.call('/channels/alert-desk/retention')
        facts['original_prune'] = rig.call('/channels/alert-desk/retention', {})

        # 05:12:00 生产方用同一生产键、同一信封重新发布 → 新消息、新身份。
        clock.at(5, 12, 0)
        facts['new_publish'] = rig.call('/channels/alert-desk/messages', ORIGINAL, 201)
        # 05:12:05 补充材料：时间接近、但落在另一条独立链路。
        clock.at(5, 12, 5)
        facts['mirror_publish'] = rig.call('/channels/alert-mirror/messages', ORIGINAL, 201)

        # 05:18:00 领取新消息（分发 3）；05:18:05 领取补充链路（分发 4）。
        clock.at(5, 18, 0)
        fresh = rig.call('/channels/alert-desk/groups/primary-desk/claim',
                         {'worker': 'desk-a', 'limit': 1})['items'][0]
        clock.at(5, 18, 5)
        mirror = rig.call('/channels/alert-mirror/groups/primary-desk/claim',
                          {'worker': 'desk-a2', 'limit': 1})['items'][0]
        facts['new_claim'] = dict(delivery_id=fresh['delivery_id'], worker='desk-a',
                                  attempt=fresh['lease']['id'], expires=fresh['lease']['expires'])
        facts['mirror_claim'] = dict(delivery_id=mirror['delivery_id'], worker='desk-a2',
                                     attempt=mirror['lease']['id'], expires=mirror['lease']['expires'])
        if fresh['delivery_id'] != NEW_DELIVERY:
            raise RuntimeError(f'新消息分发号漂移：{fresh["delivery_id"]}')
        if child['delivery_id'] != LATE_CHILD:
            raise RuntimeError(f'旧链子分发号漂移：{child["delivery_id"]}')

        # 05:21:00 唯一发生在 05:21 的处置：补充链路分发 4 被确认。新消息分发 3 仍未处置。
        clock.at(5, 21, 0)
        facts['mirror_ack'] = rig.call(
            f"/deliveries/{mirror['delivery_id']}/ack", {'token': mirror['lease']['token']})

        clock.at(5, 21, 30)
        evidence['mirror'] = rig.evidence(facts['mirror_publish']['message_id'])
        # 05:22:00 保留下来的新消息证据（此刻仍处租约中、尝试为 open）。
        clock.at(5, 22, 0)
        evidence['new'] = rig.evidence(facts['new_publish']['message_id'])

        # 05:23:30 全局回收：新消息租约在 05:23:00 到期，记为 expired（不是确认）。
        clock.at(5, 23, 30)
        facts['sweep'] = rig.call('/sweep', {'limit': 10})

        # 05:24 两条频道各自清理；05:27 终态核查。
        clock.at(5, 24, 0)
        facts['new_retention_plan'] = rig.call('/channels/alert-desk/retention')
        facts['new_prune'] = rig.call('/channels/alert-desk/retention', {})
        clock.at(5, 24, 30)
        facts['mirror_prune'] = rig.call('/channels/alert-mirror/retention', {})
        clock.at(5, 27, 0)
        facts['final_reads'] = {str(key): rig.raw(f'/messages/{key}')[0] for key in (1, 2, 3)}
        facts['final_board'] = rig.call('/channels/alert-desk')
        facts['final_retention'] = {
            'alert-desk': rig.call('/channels/alert-desk/retention'),
            'alert-mirror': rig.call('/channels/alert-mirror/retention')}
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
        directory.cleanup()

    return {'format': FORMAT, 'generated_for': '复盘账本 · 告警清理后原键复用',
            'clock_base': Clock().base, 'facts': facts, 'evidence': evidence}


def write(bundle: dict, target: Path) -> dict:
    target.mkdir(parents=True, exist_ok=True)
    manifest = {}
    facts_path = target / 'facts.json'
    facts_path.write_text(pack(bundle['facts']) + '\n', encoding='utf-8')
    manifest[facts_path.name] = {'sha256': fingerprint(facts_path.read_text(encoding='utf-8')),
                                 'digest_of': 'file'}
    for name, body in bundle['evidence'].items():
        path = target / f'evidence-{name}.json'
        path.write_bytes(body)
        record = json.loads(body)
        claimed = record.pop('sha256')
        if fingerprint(pack(record)) != claimed:
            raise RuntimeError(f'导出证据 {name} 的摘要不自洽')
        manifest[path.name] = {'sha256': claimed, 'digest_of': 'record'}
    (target / 'MANIFEST.json').write_text(
        json.dumps({'format': FORMAT, 'files': manifest}, ensure_ascii=False, indent=1) + '\n',
        encoding='utf-8')
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description='生成复盘用的同构本机样本')
    parser.add_argument('--out', type=Path, default=Path('review/evidence/run-001'))
    args = parser.parse_args(argv)
    bundle = run()
    manifest = write(bundle, args.out)
    print(f'样本已写入 {args.out}')
    for name, entry in manifest.items():
        print(f"  {name:24} {entry['sha256']}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
