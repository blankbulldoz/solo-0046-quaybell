import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import SCHEMA, VERSION

MIGRATIONS = {
    1: '''
ALTER TABLE messages ADD COLUMN deadline REAL;
ALTER TABLE deliveries ADD COLUMN terminal_reason TEXT;
CREATE INDEX IF NOT EXISTS message_deadline ON messages(deadline) WHERE deadline IS NOT NULL;
''',
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
            db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nPRAGMA user_version={VERSION};\nCOMMIT;')
            # A fresh store already matches the current schema; older stores step forward.
            if 0 < version < VERSION:
                while version < VERSION:
                    db.executescript(f'BEGIN IMMEDIATE;\n{MIGRATIONS[version]}\n'
                                     f'PRAGMA user_version={version + 1};\nCOMMIT;')
                    version += 1
            # Created after migration so older stores gain the deadline column first.
            db.execute('CREATE INDEX IF NOT EXISTS message_deadline ON messages(deadline) '
                       'WHERE deadline IS NOT NULL')

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
