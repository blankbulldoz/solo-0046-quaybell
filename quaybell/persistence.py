import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import SCHEMA, VERSION


class RelayPersistence:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version > VERSION:
                raise RuntimeError(f'无法打开更新版本产生的存储版本 {version}')
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nCOMMIT;')
            self._migrate(db, version)
            db.execute(f'PRAGMA user_version={VERSION}')

    @staticmethod
    def _migrate(db, version):
        # v1 -> v2：尝试增加失败分类，投递增加终结原因。旧记录留空，
        # 读取时按 outcome 回退解释，因此旧消费者写入的数据行为不变。
        attempt_columns = {row[1] for row in db.execute('PRAGMA table_info(attempts)')}
        if 'classification' not in attempt_columns:
            db.execute('ALTER TABLE attempts ADD COLUMN classification TEXT')
        delivery_columns = {row[1] for row in db.execute('PRAGMA table_info(deliveries)')}
        if 'terminal_reason' not in delivery_columns:
            db.execute('ALTER TABLE deliveries ADD COLUMN terminal_reason TEXT')

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, isolation_level=None, timeout=4)
        try:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA foreign_keys=ON')
            yield db
        finally:
            db.close()

    @contextmanager
    def session(self, *, write=False):
        with self._connection() as db:
            db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            try:
                yield db
                db.execute('COMMIT')
            except BaseException:
                if db.in_transaction:
                    db.execute('ROLLBACK')
                raise
