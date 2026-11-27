"""升级对照工作流：旧 v1 库（Unix 秒浮点）-> v2 库（整数毫秒）。

用一个手工构造的 v1 SQLite 库演示四类升级对照：
  1. 待领时刻带毫秒内差异（两条 ready_at 落到同一毫秒，仍按投递 ID 维持队首顺序）；
  2. 仍有效的旧租约（升级后凭据照旧可用，截止边界保持 <= 的封闭约定）；
  3. 刚到截止的签收（恰在截止时刻的旧凭据被拒、随后可回收重试）；
  4. 留存候选（按发布时保留秒数判定：合格项可清理，仍有未处置分发的同龄项保留）。
另演示升级被打断后再次启动可继续，并对迁移前后的行数、时间列求和/极值和外键做对账。

运行：python3 -m examples.upgrade
"""
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from quaybell import codec
from quaybell.evidence import export_trace
from quaybell.migration import MigrationAborted, TIME_COLUMNS, migrate
from quaybell.persistence import RelayPersistence
from quaybell.server import RelayServer

CHANNEL = 'signal-weir'
GROUP = '/channels/signal-weir/groups/rotor'
# 受控旧时钟基准；显式带亚毫秒小数，以制造毫秒内差异。
T0 = 1_730_000_000.0
T1 = 1_730_000_000.2503      # -> ROUND 到 ...000.250
T2 = 1_730_000_000.2507      # -> ROUND 到 ...000.251（与 T1 仅差 0.4ms，不共毫秒）
T3 = 1_730_000_000.2504      # -> ROUND 到 ...000.250（与 T1 同一毫秒，制造共毫秒）
LEASE_FROM = 1_730_000_005.0
LEASE_DEADLINE = 1_730_000_009.0     # 租约恰好在此刻失效（<= 封闭区间）

V1_SCHEMA = '''
CREATE TABLE channels (name TEXT PRIMARY KEY, created REAL NOT NULL);
CREATE TABLE policies (channel TEXT NOT NULL REFERENCES channels(name), version INTEGER NOT NULL,
  lease_seconds INTEGER NOT NULL, retry_seconds INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
  retention_seconds INTEGER NOT NULL, created REAL NOT NULL, PRIMARY KEY(channel, version));
CREATE TABLE subscriptions (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL REFERENCES channels(name),
  name TEXT NOT NULL, tags TEXT NOT NULL, paused INTEGER NOT NULL DEFAULT 0 CHECK(paused IN (0,1)),
  created REAL NOT NULL, UNIQUE(channel, name));
CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, producer TEXT NOT NULL,
  dedup_key TEXT NOT NULL, lane TEXT NOT NULL, envelope TEXT NOT NULL, fingerprint TEXT NOT NULL,
  policy INTEGER NOT NULL, published REAL NOT NULL, available REAL NOT NULL,
  FOREIGN KEY(channel, policy) REFERENCES policies(channel, version),
  UNIQUE(channel, producer, dedup_key));
CREATE TABLE deliveries (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL
  REFERENCES messages(id) ON DELETE CASCADE, subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
  lane TEXT NOT NULL, policy INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('waiting','leased','acked','dead')),
  ready_at REAL NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0, terminal_at REAL,
  parent_id INTEGER UNIQUE REFERENCES deliveries(id));
CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id INTEGER NOT NULL
  REFERENCES deliveries(id) ON DELETE CASCADE, number INTEGER NOT NULL, token_digest TEXT NOT NULL UNIQUE,
  worker TEXT NOT NULL, started REAL NOT NULL, expires REAL NOT NULL,
  outcome TEXT NOT NULL CHECK('open' = outcome OR outcome IN ('acked','nacked','expired')),
  finished REAL, reason TEXT, receipt TEXT, UNIQUE(delivery_id, number));
CREATE TABLE journal (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL REFERENCES channels(name),
  message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE, at REAL NOT NULL, kind TEXT NOT NULL,
  details TEXT NOT NULL);
CREATE INDEX delivery_lane ON deliveries(subscription_id,lane,id) WHERE state IN ('waiting','leased');
CREATE INDEX attempt_deadline ON attempts(expires,id) WHERE outcome='open';
CREATE UNIQUE INDEX one_open_attempt ON attempts(delivery_id) WHERE outcome='open';
'''


def _envelope(key, lane='coil-north'):
    return codec.pack(dict(lane=lane, tags={'feed': 'telemetry'}, payload={'key': key}, delay_seconds=0))


def build_legacy_database(path):
    """按 v1 合同直接写入秒级 REAL 数据，含四类对照所需的全部记录。"""
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript(V1_SCHEMA)
    db.execute('PRAGMA user_version=1')
    db.execute('INSERT INTO channels VALUES (?,?)', (CHANNEL, T0))
    db.execute('INSERT INTO policies VALUES (?,?,?,?,?,?,?)',
               (CHANNEL, 1, 30, 3, 2, 8, T0))     # 租约 30s、退避 3s、保留 8s
    db.execute('INSERT INTO subscriptions(channel,name,tags,paused,created) VALUES (?,?,?,?,?)',
               (CHANNEL, 'rotor', '{}', 0, T0))
    # 六条消息：m1/m2 制造毫秒内差异；m3 是已 ack 的留存候选；m4 同龄但仍有未处置分发；
    # m5/m6 各自承载一个开放租约（仍有效 / 刚到截止）。
    rows = [
        (1, 'pulse-millisecond-a', 'coil-north', T1, T1),
        (2, 'pulse-millisecond-b', 'coil-north', T2, T3),
        (3, 'pulse-retention-ok', 'coil-south', T0, T0),
        (4, 'pulse-retention-busy', 'coil-east', T0, T0),
        (5, 'pulse-lease-live', 'coil-west', LEASE_FROM, LEASE_FROM),
        (6, 'pulse-lease-deadline', 'coil-rim', LEASE_FROM, LEASE_DEADLINE - 4),
    ]
    for mid, key, lane, published, available in rows:
        encoded = _envelope(key, lane)
        db.execute('INSERT INTO messages(id,channel,producer,dedup_key,lane,envelope,fingerprint,policy,'
                   'published,available) VALUES (?,?,?,?,?,?,?,?,?,?)',
                   (mid, CHANNEL, 'sensor-gate', key, lane, encoded, codec.fingerprint(encoded), 1,
                    published, available))
    # d1/d2 等待中：同顺序键，ready 时刻仅差 0.1ms 且映射到同一毫秒（...250）。
    db.execute("INSERT INTO deliveries VALUES (1,1,1,'coil-north',1,'waiting',?,0,NULL,NULL)", (T1,))
    db.execute("INSERT INTO deliveries VALUES (2,2,1,'coil-north',1,'waiting',?,0,NULL,NULL)", (T3,))
    # d3 已 ack：整组无活动分发，是留存合格项（发布于 T0，保留 8s）。
    db.execute("INSERT INTO deliveries VALUES (3,3,1,'coil-south',1,'acked',?,1,?,NULL)", (T0, T0))
    # d4 仍在租约中（截止较晚，演示期间始终有效）：同龄消息必须保留。
    db.execute("INSERT INTO deliveries VALUES (4,4,1,'coil-east',1,'leased',?,1,NULL,NULL)", (LEASE_FROM,))
    # d5/d6 各自持有一个开放租约：d5 仍有效；d6 截止时刻恰为 LEASE_DEADLINE（刚到截止）。
    db.execute("INSERT INTO deliveries VALUES (5,5,1,'coil-west',1,'leased',?,1,NULL,NULL)", (LEASE_FROM,))
    db.execute("INSERT INTO deliveries VALUES (6,6,1,'coil-rim',1,'leased',?,1,NULL,NULL)",
               (LEASE_DEADLINE - 4,))
    db.execute("INSERT INTO attempts(delivery_id,number,token_digest,worker,started,expires,outcome)"
               " VALUES (?,?,?,?,?,?,'open')",
               (5, 1, codec.fingerprint('token-valid'), 'bench-a', LEASE_FROM, LEASE_FROM + 4))
    db.execute("INSERT INTO attempts(delivery_id,number,token_digest,worker,started,expires,outcome)"
               " VALUES (?,?,?,?,?,?,'open')",
               (6, 1, codec.fingerprint('token-deadline'), 'bench-b', LEASE_DEADLINE - 4, LEASE_DEADLINE))
    db.execute("INSERT INTO attempts(delivery_id,number,token_digest,worker,started,expires,outcome)"
               " VALUES (?,?,?,?,?,?,'open')",
               (4, 1, codec.fingerprint('token-busy'), 'bench-c', LEASE_FROM, LEASE_FROM + 20))
    for kind, at, details in (
            ('published', T1, {'fanout': {'rotor': 1}, 'policy': 1}),
            ('leased', LEASE_FROM, {'delivery_id': 4, 'attempt': 1, 'worker': 'bench-c',
                                    'expires': LEASE_FROM + 20}),
            ('acked', T0, {'delivery_id': 3, 'attempt': 1, 'outcome': 'acked', 'state': 'acked',
                           'effective_at': T0, 'reason': None})):
        db.execute('INSERT INTO journal(channel,message_id,at,kind,details) VALUES (?,?,?,?,?)',
                   (CHANNEL, None, at, kind, codec.pack(details)))
    db.commit()
    db.close()


def snapshot_legacy(path):
    """迁移前清单：行数与每个时间列的求和/极值（秒）。"""
    report = {}
    with closing(sqlite3.connect(path)) as db:
        for table, columns in TIME_COLUMNS.items():
            entry = {'rows': db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 'columns': {}}
            for column in columns:
                total, low, high = db.execute(
                    f'SELECT COALESCE(SUM({column}),0),MIN({column}),MAX({column}) FROM {table}').fetchone()
                entry['columns'][column] = {'sum': total, 'min': low, 'max': high}
            report[table] = entry
    return report


def _request(base, method, path, data=None):
    payload = None if data is None else json.dumps(data).encode('utf-8')
    headers = {'Content-Type': 'application/json'} if data is not None else {}
    req = Request(base + path, data=payload, headers=headers, method=method)
    try:
        response = urlopen(req, timeout=5)
    except HTTPError as exc:
        response = exc
    with response:
        return response.code, json.load(response)


def run_http_checks(db_path, clock):
    """升级后经公共 HTTP 接口完成四类对照。"""
    persistence = RelayPersistence(db_path)          # 已是 v2，直接打开不触发迁移
    with RelayServer(('127.0.0.1', 0), persistence, lambda: clock[0]) as server:
        thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
        thread.start()
        try:
            base = f'http://127.0.0.1:{server.server_port}/v1'
            findings = {}

            # 对照 1：毫秒内差异。T1 与 T3 映射到同一毫秒 ...250，ID 小的 d1 仍排前。
            clock[0] = 1_730_000_001_000
            _, board = _request(base, 'GET', f'/channels/{CHANNEL}')
            sample = board['groups'][0]['sample']
            head = next(item for item in sample if item['id'] == 1)
            tail = next(item for item in sample if item['id'] == 2)
            assert head['ready_at'] == tail['ready_at'] == 1_730_000_000.25, head['ready_at']
            assert head['cause'] == 'ready' and tail['cause'] == 'blocked'
            _, claimed = _request(base, 'POST', GROUP + '/claim', {'worker': 'bench', 'limit': 10})
            assert [item['delivery_id'] for item in claimed['items']] == [1], claimed
            assert all(isinstance(item['lease']['expires'], float) for item in claimed['items'])
            findings['sub_millisecond'] = {
                'legacy_ready_seconds': [T1, T3], 'shared_millisecond': head['ready_at'],
                'claim_order': [item['delivery_id'] for item in claimed['items']]}

            # 对照 2：仍有效的旧租约。d5 未到截止，旧凭据照常 ack。
            clock[0] = int(LEASE_FROM * 1000) + 2_000
            status, ack = _request(base, 'POST', '/deliveries/5/ack', {'token': 'token-valid'})
            assert status == 200 and ack['state'] == 'acked' and ack['repeated'] is False
            findings['live_lease'] = {'delivery': 5, 'effective_at': ack['effective_at'],
                                      'external_unit_seconds': isinstance(ack['effective_at'], float)}

            # 对照 3：刚到截止的签收。时钟拨到 d6 截止的同一毫秒，ack 必须 409；再 sweep 回收。
            clock[0] = int(LEASE_DEADLINE * 1000)
            status, body = _request(base, 'POST', '/deliveries/6/ack', {'token': 'token-deadline'})
            assert status == 409 and body['error'] == 'conflict', body
            status, swept = _request(base, 'POST', '/sweep', {})
            assert status == 200 and swept['recovered'][0]['delivery_id'] == 6
            # 超时退避从原截止时间起算 +3s：1730000009 -> 1730000012
            assert swept['recovered'][0]['effective_at'] == LEASE_DEADLINE + 3
            findings['deadline_lease'] = {'at': LEASE_DEADLINE, 'ack_status': 409,
                                          'retry_ready_at': swept['recovered'][0]['effective_at']}

            # 对照 4：留存候选。时钟越过 T0+8s；d3 合格可清理，d4 因有活动历史同龄保留。
            clock[0] = 1_730_000_009_500
            _, plan = _request(base, 'GET', f'/channels/{CHANNEL}/retention')
            assert [row['id'] for row in plan['eligible']] == [3], plan
            assert all(isinstance(row['cutoff'], float) for row in plan['eligible'])
            status, pruned = _request(base, 'POST', f'/channels/{CHANNEL}/retention', {})
            assert status == 200 and pruned['removed'] == [3]
            status, _ = _request(base, 'GET', '/messages/3')
            assert status == 404
            status, still = _request(base, 'GET', '/messages/4')
            assert status == 200
            findings['retention'] = {'removed': [3], 'kept_with_open_history': [4],
                                      'observed_at': pruned['observed_at']}
            return findings
        finally:
            server.shutdown()
            thread.join()


def compare_evidence(db_path, clock, legacy_published):
    """历史可比性：导出格式不变、摘要仍可验、时间按旧合同（Unix 秒）呈现。

    旧库的亚毫秒尾数在整数毫秒存储下按最近毫秒收缩；导出仍给出同格式同单位的秒值，
    与迁移前清单的差异不超过 0.5ms，历史事实（状态、顺序、ID）完全不变。
    """
    persistence = RelayPersistence(db_path)
    with persistence.session() as db:
        exported = export_trace(db, 1, clock[0])
    doc = json.loads(exported)
    assert doc['format'] == 'quaybell-trace/1'
    digest = doc.pop('sha256')
    assert codec.fingerprint(codec.pack(doc)) == digest          # 摘要自洽
    assert isinstance(doc['observed_at'], float)
    message = doc['trace']['message']
    assert isinstance(message['published'], float)
    rounded = round(legacy_published * 1000) / 1000              # 迁移采用的最近毫秒映射
    assert message['published'] == rounded
    assert abs(message['published'] - legacy_published) <= 0.0005 + 1e-9
    # 历史结构不变：消息、每条派生分发及其尝试仍在同一证据快照内，租约凭据不进入导出。
    trace = doc['trace']
    assert {'message', 'deliveries', 'events'} <= set(trace)
    assert 'token' not in json.dumps(trace)
    delivery_ids = [item['id'] for item in trace['deliveries']]
    assert delivery_ids == [1]
    # 即便已领取，导出的尝试记录只含公开字段，租约凭据（token）从不进入证据。
    for item in trace['deliveries']:
        for attempt in item['attempts']:
            assert 'token' not in attempt and 'token_digest' not in attempt
    return {'format': doc['format'], 'sha256': digest,
            'observed_at_seconds': doc['observed_at'],
            'published_legacy_seconds': legacy_published,
            'published_exported_seconds': message['published'],
            'max_rounding_ms': 0.5}


def cross_check(path, legacy):
    """迁移前后秒级对账：行数不变，毫秒值换算回秒后求和/极值偏差不超过半个毫秒。"""
    with closing(sqlite3.connect(path)) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 2
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name LIKE '%__legacy'").fetchall()
        assert db.execute('PRAGMA foreign_key_check').fetchall() == []
        for table, columns in TIME_COLUMNS.items():
            rows = db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            assert rows == legacy[table]['rows'], table
            for column in columns:
                total, low, high = db.execute(
                    f'SELECT COALESCE(SUM({column}),0),MIN({column}),MAX({column}) FROM {table}').fetchone()
                old = legacy[table]['columns'][column]
                assert abs(total / 1000 - old['sum']) <= rows * 0.0005 + 1e-9, (table, column)
                if old['min'] is not None:
                    assert abs(low / 1000 - old['min']) <= 0.0005 + 1e-9, (table, column)
                    assert abs(high / 1000 - old['max']) <= 0.0005 + 1e-9, (table, column)


def reconcile(path, migrated, report):
    """HTTP 演练后核对：结构保持 v2 整数毫秒约定，行数变化与合同动作一致。"""
    with closing(sqlite3.connect(path)) as db:
        version = db.execute('PRAGMA user_version').fetchone()[0]
        assert version == 2
        assert db.execute('PRAGMA foreign_key_check').fetchall() == []
        # HTTP 演练按合同新增/删除的行：领取/ack/超时/清理各写一条日志；
        # 留存清理 m3 级联删除其分发。
        extra_rows = {'attempts': 1, 'journal': 4, 'deliveries': -1, 'messages': -1}
        for table, columns in TIME_COLUMNS.items():
            rows = db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            assert rows == migrated[table]['rows'] + extra_rows.get(table, 0), table
            for column in columns:
                # 演练新增/删除行不改变存储事实：所有时间值仍是整数毫秒。
                bad = db.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND typeof({column})!='integer'"
                ).fetchone()[0]
                assert bad == 0, (table, column)
        # 迁移自身的 verify 报告覆盖演练前的逐列求和/极值对账，确认其覆盖全部七张表。
        assert set(report['reconciliation']) == set(TIME_COLUMNS)
    return {'user_version': version,
            'tables': sorted(report['reconciliation']),
            'row_totals_at_migration': {table: migrated[table]['rows'] for table in TIME_COLUMNS}}


def main():
    with TemporaryDirectory(prefix='quaybell-upgrade-') as directory:
        db_path = Path(directory) / 'relay.sqlite3'
        build_legacy_database(db_path)
        legacy = snapshot_legacy(db_path)

        # 第一次启动迁移，在 rebuild:attempts 之后注入崩溃，模拟升级被打断。
        try:
            migrate(db_path, crash_after='rebuild:attempts')
        except MigrationAborted as exc:
            interrupted_step = exc.args[0]
        with closing(sqlite3.connect(db_path)) as db:
            assert db.execute('PRAGMA user_version').fetchone()[0] == 1   # 未完成，版本号不动
            partial = {row[0] for row in db.execute('SELECT step FROM migration_ledger')}
        assert 'rebuild:attempts' in partial and 'verify' not in partial

        # 再次启动：从断点继续，已完成步骤跳过。
        resumed = migrate(db_path)
        assert resumed['steps'][0] == 'rebuild:deliveries' or 'rebuild:attempts' not in resumed['steps']
        assert 'rebuild:attempts' not in resumed['steps']                  # 断点前的步骤没有重跑
        assert 'version' in resumed['steps']

        # 迁移完成、HTTP 演练前：用独立连接把毫秒值换算回秒，与迁移前清单逐项对账。
        cross_check(db_path, legacy)
        migrated = snapshot_legacy(db_path)                               # 演练前行数基准

        clock = [1_730_000_001_000]
        findings = run_http_checks(db_path, clock)
        evidence = compare_evidence(db_path, clock, T1)
        accounts = reconcile(db_path, migrated, resumed)
        report = {'interrupted_at': interrupted_step, 'resumed_steps': resumed['steps'],
                  'http_controls': findings, 'evidence': evidence, 'reconciliation': accounts}
        print('upgrade 通过 ' + codec.pack(report))


if __name__ == '__main__':
    main()
