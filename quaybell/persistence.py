import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import SCHEMA, VERSION

# Existing databases gain subscription rule versions in place. Each group's
# current tags become rule version 1, identical to what it has been matching
# with, so starting a comparison never silently moves the baseline.
MIGRATIONS = {
    1: '''
    ALTER TABLE subscriptions ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
    INSERT INTO subscription_rules(subscription_id,version,tags,created)
      SELECT id,1,tags,created FROM subscriptions;
    ''',
}


class RelayPersistence:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, VERSION):
                raise RuntimeError(f'无法打开存储版本 {version}')
            db.execute('PRAGMA journal_mode=WAL')
            steps = [f'BEGIN IMMEDIATE;\n{SCHEMA}']
            if version in MIGRATIONS:
                steps.append(MIGRATIONS[version])
            db.executescript(f'{"".join(steps)}\nPRAGMA user_version={VERSION};\nCOMMIT;')

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
