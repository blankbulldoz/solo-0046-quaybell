import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import SCHEMA, VERSION

# Columns added after version 1, applied idempotently to older database files.
ATTEMPT_COLUMNS = {
    'budget_expires': 'REAL',
    'lease_step': 'REAL',
    'renew_count': 'INTEGER NOT NULL DEFAULT 0',
}


class RelayPersistence:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version > VERSION:
                raise RuntimeError(f'无法打开存储版本 {version}')
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript(SCHEMA)
            present = {row[1] for row in db.execute('PRAGMA table_info(attempts)')}
            additions = [f'ALTER TABLE attempts ADD COLUMN {name} {decl}'
                         for name, decl in ATTEMPT_COLUMNS.items() if name not in present]
            if additions or version != VERSION:
                db.executescript('BEGIN IMMEDIATE;\n' + ';\n'.join(additions)
                                 + (';\n' if additions else '')
                                 + f'PRAGMA user_version={VERSION};\nCOMMIT;')

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
