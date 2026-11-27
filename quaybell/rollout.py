"""滚动上线的共库绑定与新鲜度栅栏。

威胁模型
--------
滚动替换时两个 Quaybell 进程必须指向同一份共同 SQLite 库。验收前会故意
保留一份从共同库复制的旧副本；复制瞬间它与共库逐行一致（频道、消息、
尚未到期的租约都相同），所以健康检查、频道名、条数甚至当时的租约内容
都无法发现进程误连了旧副本。

本模块给上线命令增加三道启动前核对（全程只读候选库，绝不替误连路径建库）：

1. ``path_pin`` 路径钉：候选库 realpath 必须等于 ``--common-db``，
   缺省时等于栅栏文件记录的共库 realpath；
2. ``binding`` / ``cluster``：库内 ``rollout_binding`` 必须是受管共同库，
   且集群标识一致（静态身份相同的复制件过得了这一关，故还需要第 3 关）；
3. ``fence_row`` / ``fence_message`` / ``journal_watermark`` 新鲜事实：
   候选库必须能看到由“未重启端刚刚提交的事实”生成的栅栏——指定消息行、
   其内容指纹、journal 发布事件与水位、以及栅栏行本身。

旧副本看不到复制 fork 之后才提交的栅栏事实，因此会以 ``STALE_REPLICA``
被拒绝，即使它被复制回权威路径也一样。

退出码约定（上线脚本可据此判断）：

* 0  通过；
* 2  候选库文件不存在（拒绝隐式建空库）；
* 3  ``DB_PATH_MISMATCH`` 候选路径不是权威共同库路径；
* 4  ``STALE_REPLICA`` 路径正确但缺少栅栏新鲜事实，是落后的旧副本；
* 5  ``NOT_MANAGED`` / ``CLUSTER_MISMATCH`` 没有共库绑定或集群不符；
* 6  栅栏文件本身无法读取或格式错误。
"""

import json
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import quote

EXIT_OK = 0
EXIT_DB_MISSING = 2
EXIT_PATH_MISMATCH = 3
EXIT_STALE = 4
EXIT_UNMANAGED = 5
EXIT_BAD_FENCE = 6

BINDING_DDL = '''
CREATE TABLE IF NOT EXISTS rollout_binding (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  cluster TEXT NOT NULL,
  canonical_realpath TEXT NOT NULL,
  created REAL NOT NULL
)
'''

FENCE_DDL = '''
CREATE TABLE IF NOT EXISTS rollout_fence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nonce TEXT NOT NULL UNIQUE,
  channel TEXT,
  message_id INTEGER NOT NULL,
  fingerprint TEXT NOT NULL,
  journal_watermark INTEGER NOT NULL,
  committed_via TEXT,
  note TEXT,
  created REAL NOT NULL
)
'''

SPEC_VERSION = 1


class FenceFileError(Exception):
    """栅栏文件缺失或不合法。"""


def real_path(path):
    return Path(path).expanduser().resolve()


def _connect_rw(path):
    db = sqlite3.connect(str(path), timeout=4)
    db.execute('PRAGMA busy_timeout=4000')
    db.row_factory = sqlite3.Row
    return db


def init_cluster(path, cluster, now=None):
    """在共同库中建立（或核对）唯一的上线绑定。返回绑定行。"""
    if not cluster or not isinstance(cluster, str):
        raise ValueError('cluster 必须是非空字符串')
    path = real_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time() if now is None else now
    db = _connect_rw(path)
    try:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute(BINDING_DDL)
        db.execute(FENCE_DDL)
        row = db.execute('SELECT * FROM rollout_binding WHERE id=1').fetchone()
        if row is None:
            db.execute('INSERT INTO rollout_binding(id,cluster,canonical_realpath,created) VALUES (1,?,?,?)',
                       (cluster, str(path), now))
            db.commit()
            row = db.execute('SELECT * FROM rollout_binding WHERE id=1').fetchone()
        elif row['cluster'] != cluster:
            raise ValueError(f"库已绑定集群 {row['cluster']!r}，与请求的 {cluster!r} 冲突")
        return dict(row)
    finally:
        db.close()


def record_fence(path, *, cluster, nonce, channel, message_id, committed_via, note=None, now=None):
    """读取伙伴端刚提交的消息事实与 journal 水位，并落一条栅栏行。"""
    path = real_path(path)
    now = time.time() if now is None else now
    db = _connect_rw(path)
    try:
        db.execute(BINDING_DDL)
        db.execute(FENCE_DDL)
        message = db.execute('SELECT fingerprint, channel FROM messages WHERE id=?', (message_id,)).fetchone()
        if message is None:
            raise ValueError(f'共同库中不存在消息 {message_id}')
        watermark = db.execute('SELECT COALESCE(MAX(id),0) FROM journal').fetchone()[0]
        cur = db.execute('''INSERT INTO rollout_fence
                            (nonce,channel,message_id,fingerprint,journal_watermark,committed_via,note,created)
                            VALUES (?,?,?,?,?,?,?,?)''',
                         (nonce, channel, message_id, message['fingerprint'], watermark,
                          committed_via, note, now))
        db.commit()
        fence_id = cur.lastrowid
        return dict(fence_id=fence_id, fingerprint=message['fingerprint'],
                    journal_watermark=watermark, db_realpath=str(path), cluster=cluster,
                    channel=channel, message_id=message_id, nonce=nonce,
                    committed_via=committed_via, note=note, created=now,
                    spec_version=SPEC_VERSION)
    finally:
        db.close()


def write_spec(spec, out_path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2, sort_keys=True) + '\n',
                        encoding='utf-8')
    return real_path(out_path)


def load_spec(path):
    path = Path(path)
    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise FenceFileError(f'无法读取栅栏文件 {path}: {exc}') from None
    required = ('cluster', 'db_realpath', 'fence_id', 'nonce', 'message_id',
                'fingerprint', 'journal_watermark')
    missing = [key for key in required if key not in raw]
    if missing:
        raise FenceFileError(f'栅栏文件缺少字段: {",".join(missing)}')
    if raw.get('spec_version', SPEC_VERSION) != SPEC_VERSION:
        raise FenceFileError(f"不支持的栅栏版本 {raw.get('spec_version')!r}")
    return raw


def _connect_readonly(path):
    """优先只读 URI；WAL/shm 受限环境下降级为 query_only 连接（仍不写入）。"""
    uri = quote(str(real_path(path)))
    try:
        db = sqlite3.connect(f'file:{uri}?mode=ro', uri=True, timeout=4)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=4000')
        db.execute('SELECT 1 FROM sqlite_master').fetchone()
        return db
    except sqlite3.OperationalError:
        db = sqlite3.connect(str(path), timeout=4)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=4000')
        db.execute('PRAGMA query_only=ON')
        return db


def evaluate(db_path, *, common_db=None, expected_cluster=None, spec=None):
    """对候选库执行全部核对，返回结构化报告；不抛“核对失败”类异常。"""
    candidate = real_path(db_path)
    pin = real_path(common_db) if common_db else (real_path(spec['db_realpath']) if spec else None)
    cluster = expected_cluster or (spec.get('cluster') if spec else None)
    report = {
        'ok': False,
        'code': None,
        'exit_code': EXIT_STALE,
        'candidate_realpath': str(candidate),
        'pin_realpath': str(pin) if pin else None,
        'expected_cluster': cluster,
        'checks': {},
        'observed': {},
        'failures': [],
    }

    top = {'rank': -1}

    def fail(check, code, message, exit_code):
        rank = {'DB_NOT_FOUND': 100, 'DB_PATH_MISMATCH': 90,
                'NOT_MANAGED': 80, 'CLUSTER_MISMATCH': 80,
                'FENCE_FILE': 70, 'STALE_REPLICA': 40, 'FINGERPRINT_MISMATCH': 40}.get(code, 10)
        report['checks'][check] = 'fail'
        report['failures'].append({'check': check, 'code': code, 'message': message})
        if rank > top['rank']:
            top['rank'] = rank
            report['code'] = code
            report['exit_code'] = exit_code

    if not candidate.is_file():
        report['checks']['db_file'] = 'fail'
        fail('db_file', 'DB_NOT_FOUND', f'候选库文件不存在: {candidate}（上线命令不会替误连路径创建空库）',
             EXIT_DB_MISSING)
        report['ok'] = False
        return report
    report['checks']['db_file'] = 'pass'

    if pin is not None:
        if candidate == pin:
            report['checks']['path_pin'] = 'pass'
        else:
            fail('path_pin', 'DB_PATH_MISMATCH',
                 f'候选库 {candidate} 不是权威共同库 {pin}；这通常意味着 --db 指向了复制旧库',
                 EXIT_PATH_MISMATCH)
    else:
        report['checks']['path_pin'] = 'skipped'

    try:
        db = _connect_readonly(candidate)
    except sqlite3.DatabaseError as exc:
        fail('sqlite_open', 'NOT_MANAGED', f'候选库无法作为 SQLite 库打开: {exc}', EXIT_UNMANAGED)
        return report
    try:
        has_binding = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='rollout_binding'").fetchone()
        row = db.execute('SELECT * FROM rollout_binding WHERE id=1').fetchone() if has_binding else None
        if row is None:
            report['checks']['binding'] = 'fail'
            fail('binding', 'NOT_MANAGED',
                 '候选库没有 rollout_binding 共库绑定；它不是由上线集群初始化的那份共同库',
                 EXIT_UNMANAGED)
        else:
            report['checks']['binding'] = 'pass'
            report['observed']['binding_cluster'] = row['cluster']
            report['observed']['canonical_realpath'] = row['canonical_realpath']
            if cluster and row['cluster'] != cluster:
                report['checks']['cluster'] = 'fail'
                fail('cluster', 'CLUSTER_MISMATCH',
                     f"候选库集群 {row['cluster']!r} 与上线要求 {cluster!r} 不符", EXIT_UNMANAGED)
            else:
                report['checks']['cluster'] = 'pass'

        journal_max = 0
        tables = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if 'journal' in tables:
            journal_max = db.execute('SELECT COALESCE(MAX(id),0) FROM journal').fetchone()[0]
        report['observed']['journal_max'] = journal_max
        report['observed']['core_schema'] = all(
            name in tables for name in ('messages', 'journal', 'attempts'))

        if spec:
            fence_row = None
            if 'rollout_fence' in tables:
                fence_row = db.execute('SELECT * FROM rollout_fence WHERE id=? AND nonce=?',
                                       (spec['fence_id'], spec['nonce'])).fetchone()
            if fence_row is None:
                report['checks']['fence_row'] = 'fail'
                fail('fence_row', 'STALE_REPLICA',
                     f"候选库看不到栅栏行 #{spec['fence_id']}；该栅栏产生于复制 fork 之后，"
                     '说明候选库是落后的旧副本，而不是状态已推进的共库', EXIT_STALE)
            else:
                report['checks']['fence_row'] = 'pass'

            message = None
            if 'messages' in tables:
                message = db.execute('SELECT channel,fingerprint FROM messages WHERE id=?',
                                     (spec['message_id'],)).fetchone()
            if message is None:
                report['checks']['fence_message'] = 'fail'
                fail('fence_message', 'STALE_REPLICA',
                     f"候选库看不到未重启端刚提交的事实：消息 #{spec['message_id']} 不存在", EXIT_STALE)
            elif message['fingerprint'] != spec['fingerprint']:
                report['checks']['fence_message'] = 'fail'
                fail('fence_message', 'FINGERPRINT_MISMATCH',
                     f"消息 #{spec['message_id']} 内容指纹与栅栏不符（候选库可能被改写或来自异源）",
                     EXIT_STALE)
            else:
                report['checks']['fence_message'] = 'pass'
                report['observed']['fence_message_id'] = spec['message_id']
                report['observed']['fence_fingerprint_prefix'] = spec['fingerprint'][:12]

            if 'journal' not in tables or journal_max < spec['journal_watermark']:
                report['checks']['journal_watermark'] = 'fail'
                fail('journal_watermark', 'STALE_REPLICA',
                     f"候选库 journal 水位 {journal_max} 低于栅栏水位 "
                     f"{spec['journal_watermark']}；共库已推进而候选库停在旧快照", EXIT_STALE)
            else:
                event = db.execute(
                    "SELECT 1 FROM journal WHERE message_id=? AND kind='published' LIMIT 1",
                    (spec['message_id'],)).fetchone()
                if event is None:
                    report['checks']['journal_watermark'] = 'fail'
                    fail('journal_watermark', 'STALE_REPLICA',
                         '候选库 journal 水位足够，但缺少栅栏消息的 published 事件', EXIT_STALE)
                else:
                    report['checks']['journal_watermark'] = 'pass'
    finally:
        db.close()

    if not report['failures']:
        report['ok'] = True
        report['code'] = 'OK'
        report['exit_code'] = EXIT_OK
    return report


def emit_reject(report, stream=None):
    stream = stream or sys.stderr
    stream.write('QB_GUARD_REJECT ' + json.dumps(
        {'code': report['code'], 'exit_code': report['exit_code'],
         'candidate': report['candidate_realpath'], 'pin': report['pin_realpath'],
         'failures': report['failures']}, ensure_ascii=False) + '\n')
    for item in report['failures']:
        stream.write(f"拒绝启动 Quaybell：[{item['check']}] {item['message']}\n")
    stream.flush()


def emit_pass(report, spec=None, stream=None):
    stream = stream or sys.stdout
    fence = f" fence=#{spec['fence_id']}" if spec else ''
    stream.write('QB_GUARD_PASS ' + json.dumps(
        {'cluster': report.get('expected_cluster'), 'candidate': report['candidate_realpath'],
         'journal_max': report['observed'].get('journal_max'), 'fence': fence.strip()},
        ensure_ascii=False) + '\n')
    stream.flush()


def enforce_for_cli(*, db_path, common_db=None, expected_cluster=None, fence_path=None):
    """供 ``python -m quaybell`` 启动前调用；不通过直接 sys.exit。"""
    spec = None
    if fence_path is not None:
        try:
            spec = load_spec(fence_path)
        except FenceFileError as exc:
            sys.stderr.write('QB_GUARD_REJECT ' + json.dumps(
                {'code': 'BAD_FENCE', 'exit_code': EXIT_BAD_FENCE, 'message': str(exc)},
                ensure_ascii=False) + '\n')
            sys.exit(EXIT_BAD_FENCE)
        if not expected_cluster:
            expected_cluster = spec.get('cluster')
    report = evaluate(db_path, common_db=common_db, expected_cluster=expected_cluster, spec=spec)
    if report['ok']:
        emit_pass(report, spec)
        return report
    emit_reject(report)
    sys.exit(report['exit_code'])
