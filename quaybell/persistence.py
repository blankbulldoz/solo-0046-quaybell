import sqlite3
from contextlib import contextmanager
from pathlib import Path

from . import migration
from .schema import SCHEMA, VERSION


class RelayPersistence:
    def __init__(self, path, *, crash_after=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, VERSION):
                raise RuntimeError(f'无法打开存储版本 {version}')
            db.execute('PRAGMA journal_mode=WAL')
            if version == VERSION:
                db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nCOMMIT;')
            elif version == 1:
                # 旧库（或被打断、停在中间状态的库）走分阶段迁移；ledger 保证可续跑。
                db.close()
                migration.migrate(self.path, crash_after=crash_after)
                db = sqlite3.connect(self.path, isolation_level=None)
                db.row_factory = sqlite3.Row
                if db.execute('PRAGMA user_version').fetchone()[0] != VERSION:
                    raise RuntimeError('迁移结束后存储版本仍不正确')
                db.execute('PRAGMA journal_mode=WAL')
                db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nCOMMIT;')
                db.close()
            else:
                db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nPRAGMA user_version={VERSION};\nCOMMIT;')

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
