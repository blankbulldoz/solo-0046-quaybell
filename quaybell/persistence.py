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
            if version not in (0, VERSION):
                raise RuntimeError(f'无法打开存储版本 {version}')
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nPRAGMA user_version={VERSION};\nCOMMIT;')
            self._migrate(db)

    @staticmethod
    def _migrate(db):
        columns = {row[1] for row in db.execute('PRAGMA table_info(attempts)')}
        if 'assignment_version' not in columns:
            db.execute('ALTER TABLE attempts ADD COLUMN assignment_version INTEGER')
        if 'partition' not in columns:
            db.execute('ALTER TABLE attempts ADD COLUMN partition INTEGER')
        channel_columns = {row[1] for row in db.execute('PRAGMA table_info(channels)')}
        if 'partition_count' not in channel_columns:
            db.execute('ALTER TABLE channels ADD COLUMN partition_count INTEGER')

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
