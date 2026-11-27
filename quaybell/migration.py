"""v1（Unix 秒浮点 REAL）到 v2（Unix 整数毫秒 INTEGER）的分阶段迁移。

设计要点：
- 每一步在独立写事务内提交，进度写入 migration_ledger；进程被打断后再次打开存储时，
  已完成步骤直接跳过，从断点继续。
- 迁移前记录各表行数与时间列的求和/极值清单，迁移后换算回秒重新核对（对账）。
- 旧秒值用 ROUND(x*1000) 映射到最近毫秒；亚毫秒差异因此可能落到同一毫秒，
  顺序与边界仍由原有整数 ID 和 <= / <=now 的封闭区间约定保持（见 README 升级说明）。
- journal.details 与 attempts.receipt 的规范化 JSON 内嵌时间戳（expires、effective_at）
  随所在表一并改写，保持包内单位与新列一致。
"""
import json
import sqlite3

from . import clockwork, codec
from .schema import SCHEMA, VERSION

# 每张表的时间列（NULL 可空列同样登记）。
TIME_COLUMNS = {
    'channels': ('created',),
    'policies': ('created',),
    'subscriptions': ('created',),
    'messages': ('published', 'available'),
    'deliveries': ('ready_at', 'terminal_at'),
    'attempts': ('started', 'expires', 'finished'),
    'journal': ('at',),
}
ALL_TABLES = tuple(TIME_COLUMNS)

# v2 重建表结构（与 schema.SCHEMA 保持一致，显式列出以便整表替换）。
TABLE_DDL = {
    'channels': '''CREATE TABLE channels (
  name TEXT PRIMARY KEY,
  created INTEGER NOT NULL)''',
    'policies': '''CREATE TABLE policies (
  channel TEXT NOT NULL REFERENCES channels(name),
  version INTEGER NOT NULL,
  lease_seconds INTEGER NOT NULL,
  retry_seconds INTEGER NOT NULL,
  max_attempts INTEGER NOT NULL,
  retention_seconds INTEGER NOT NULL,
  created INTEGER NOT NULL,
  PRIMARY KEY(channel, version))''',
    'subscriptions': '''CREATE TABLE subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL REFERENCES channels(name),
  name TEXT NOT NULL,
  tags TEXT NOT NULL,
  paused INTEGER NOT NULL DEFAULT 0 CHECK(paused IN (0,1)),
  created INTEGER NOT NULL,
  UNIQUE(channel, name))''',
    'messages': '''CREATE TABLE messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,
  producer TEXT NOT NULL,
  dedup_key TEXT NOT NULL,
  lane TEXT NOT NULL,
  envelope TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  policy INTEGER NOT NULL,
  published INTEGER NOT NULL,
  available INTEGER NOT NULL,
  FOREIGN KEY(channel, policy) REFERENCES policies(channel, version),
  UNIQUE(channel, producer, dedup_key))''',
    'deliveries': '''CREATE TABLE deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
  lane TEXT NOT NULL,
  policy INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('waiting','leased','acked','dead')),
  ready_at INTEGER NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  terminal_at INTEGER,
  parent_id INTEGER UNIQUE REFERENCES deliveries(id))''',
    'attempts': '''CREATE TABLE attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id INTEGER NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  number INTEGER NOT NULL,
  token_digest TEXT NOT NULL UNIQUE,
  worker TEXT NOT NULL,
  started INTEGER NOT NULL,
  expires INTEGER NOT NULL,
  outcome TEXT NOT NULL CHECK('open' = outcome OR outcome IN ('acked','nacked','expired')),
  finished INTEGER,
  reason TEXT,
  receipt TEXT,
  UNIQUE(delivery_id, number))''',
    'journal': '''CREATE TABLE journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL REFERENCES channels(name),
  message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
  at INTEGER NOT NULL,
  kind TEXT NOT NULL,
  details TEXT NOT NULL)''',
}

# 整表复制时的列顺序（不含时间换算表达式以外的任何结构变化）。
COLUMNS = {
    'channels': ('name', 'created'),
    'policies': ('channel', 'version', 'lease_seconds', 'retry_seconds', 'max_attempts',
                 'retention_seconds', 'created'),
    'subscriptions': ('id', 'channel', 'name', 'tags', 'paused', 'created'),
    'messages': ('id', 'channel', 'producer', 'dedup_key', 'lane', 'envelope', 'fingerprint',
                 'policy', 'published', 'available'),
    'deliveries': ('id', 'message_id', 'subscription_id', 'lane', 'policy', 'state', 'ready_at',
                   'attempt_count', 'terminal_at', 'parent_id'),
    'attempts': ('id', 'delivery_id', 'number', 'token_digest', 'worker', 'started', 'expires',
                 'outcome', 'finished', 'reason', 'receipt'),
    'journal': ('id', 'channel', 'message_id', 'at', 'kind', 'details'),
}

EMBEDDED_TIME_KEYS = ('expires', 'effective_at')


class MigrationAborted(RuntimeError):
    """测试用故障注入：模拟升级进程在某一步之后被打断。"""


def _millis(expr):
    # 半偶舍入对本系统的正时间戳与 ROUND(x*1000) 最近毫秒语义一致。
    return f'CAST(ROUND({expr}*{clockwork.MILLIS_PER_SECOND}) AS INTEGER)'


def _completed(db):
    return {row[0] for row in db.execute('SELECT step FROM migration_ledger')}


def _record(db, step, now_ms, detail):
    db.execute('INSERT INTO migration_ledger(step,finished_at,detail) VALUES (?,?,?)',
               (step, now_ms, codec.pack(detail)))


def _inventory(db):
    report = {}
    for table, columns in TIME_COLUMNS.items():
        entry = {'rows': db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 'columns': {}}
        for column in columns:
            stats = db.execute(
                f'SELECT COALESCE(SUM({column}),0),MIN({column}),MAX({column}) FROM {table}').fetchone()
            entry['columns'][column] = {'sum_seconds': stats[0], 'min_seconds': stats[1],
                                        'max_seconds': stats[2]}
        report[table] = entry
    return report


def _rewrite_embedded(db, table, text_column):
    """改写规范化 JSON 文本中内嵌的秒级时间戳。"""
    rows = db.execute(f'SELECT rowid,{text_column} FROM {table} WHERE {text_column} IS NOT NULL').fetchall()
    for row_id, raw in rows:
        payload = codec.unpack(raw)
        changed = False
        if isinstance(payload, dict):
            for key in EMBEDDED_TIME_KEYS:
                if key in payload and payload[key] is not None:
                    payload = payload | {key: int(round(payload[key] * clockwork.MILLIS_PER_SECOND))}
                    changed = True
        if changed:
            db.execute(f'UPDATE {table} SET {text_column}=? WHERE rowid=?', (codec.pack(payload), row_id))


def _rebuild(db, table):
    time_cols = TIME_COLUMNS[table]
    select_exprs = [_millis(col) if col in time_cols else col for col in COLUMNS[table]]
    db.execute(f'ALTER TABLE {table} RENAME TO {table}__legacy')
    db.execute(TABLE_DDL[table])
    db.execute(f'INSERT INTO {table} ({",".join(COLUMNS[table])}) '
               f'SELECT {",".join(select_exprs)} FROM {table}__legacy')
    if table == 'attempts':
        _rewrite_embedded(db, 'attempts', 'receipt')
    if table == 'journal':
        _rewrite_embedded(db, 'journal', 'details')
    db.execute(f'DROP TABLE {table}__legacy')


def _verify(db, inventory, now_ms):
    report = {}
    for table, columns in TIME_COLUMNS.items():
        current_rows = db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        if current_rows != inventory[table]['rows']:
            raise RuntimeError(f'{table} 行数对账失败：{inventory[table]["rows"]} -> {current_rows}')
        entry = {'rows': current_rows, 'columns': {}}
        for column in columns:
            legacy = inventory[table]['columns'][column]
            stats = db.execute(
                f'SELECT COALESCE(SUM({column}),0),MIN({column}),MAX({column}) FROM {table}').fetchone()
            sum_seconds, min_ms, max_ms = stats
            # 每个值最多半个毫秒的舍入差，按行累计给出容差。
            tolerance = current_rows * 0.0005 + 1e-9
            if abs(sum_seconds / 1000 - legacy['sum_seconds']) > tolerance:
                raise RuntimeError(f'{table}.{column} 求和对账失败')
            if legacy['min_seconds'] is not None:
                for bound_ms, legacy_bound in ((min_ms, legacy['min_seconds']),
                                               (max_ms, legacy['max_seconds'])):
                    if abs(bound_ms / 1000 - legacy_bound) > 0.0005 + 1e-9:
                        raise RuntimeError(f'{table}.{column} 极值对账失败')
            bad_type = db.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND typeof({column})!='integer'"
            ).fetchone()[0]
            if bad_type:
                raise RuntimeError(f'{table}.{column} 仍存在非整数毫秒值')
            entry['columns'][column] = {'sum_seconds': sum_seconds / 1000,
                                        'min_ms': min_ms, 'max_ms': max_ms}
        report[table] = entry
    violations = db.execute('PRAGMA foreign_key_check').fetchall()
    if violations:
        raise RuntimeError(f'外键对账失败：{violations[:3]}')
    return report


def _steps(db):
    """生成 (step, 回调) 列表；顺序固定，断点续跑依赖这个顺序。

    indexes 与 verify 在 migrate() 中单独处理（分别涉及 executescript 与 inventory），
    这里给 None 占位。
    """
    def rebuild(table):
        return lambda: _rebuild(db, table)

    steps = [('inventory', lambda: _inventory(db))]
    steps += [(f'rebuild:{table}', rebuild(table)) for table in ALL_TABLES]
    steps.append(('indexes', None))
    steps.append(('verify', None))
    steps.append(('version', lambda: db.execute(f'PRAGMA user_version={VERSION}')))
    return steps


def migrate(path, *, crash_after=None, clock=clockwork.now_ms):
    """把 path 上的 v1 库迁移到 v2。返回 {'steps': [...], 'reconciliation': {...}}。"""
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.row_factory = sqlite3.Row
        # 整表重建期间关闭外键强制；verify 阶段用 foreign_key_check 显式核对。
        db.execute('PRAGMA foreign_keys=OFF')
        db.execute('BEGIN IMMEDIATE')
        db.execute('''CREATE TABLE IF NOT EXISTS migration_ledger (
          step TEXT PRIMARY KEY, finished_at INTEGER NOT NULL, detail TEXT NOT NULL)''')
        db.execute('COMMIT')
        done = _completed(db)
        executed = []
        inventory = None
        if 'inventory' in done:
            raw = db.execute("SELECT detail FROM migration_ledger WHERE step='inventory'").fetchone()[0]
            inventory = json.loads(raw)
        for step, action in _steps(db):
            if step in done:
                continue
            db.execute('BEGIN IMMEDIATE')
            if step == 'indexes':
                # executescript 会先隐式提交当前事务并自行执行；其后另开事务登记断点。
                db.executescript(SCHEMA)
                db.execute('BEGIN IMMEDIATE')
                detail = {'ok': True}
            elif step == 'inventory':
                inventory = action()
                detail = inventory
            elif step == 'verify':
                detail = _verify(db, inventory, clock())
            else:
                action()
                detail = {'ok': True}
            _record(db, step, clock(), detail)
            db.execute('COMMIT')
            executed.append(step)
            if crash_after == step:
                raise MigrationAborted(step)
        return {'steps': executed,
                'reconciliation': json.loads(
                    db.execute("SELECT detail FROM migration_ledger WHERE step='verify'").fetchone()[0])}
    except BaseException:
        if db.in_transaction:
            db.execute('ROLLBACK')
        raise
    finally:
        db.close()
