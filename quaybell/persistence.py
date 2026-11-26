import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import SCHEMA, VERSION

DELIVERIES_V2 = '''CREATE TABLE deliveries_v2 (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
  lane TEXT NOT NULL,
  policy INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('waiting','leased','acked','dead','superseded')),
  ready_at REAL NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  terminal_at REAL,
  parent_id INTEGER UNIQUE REFERENCES deliveries(id),
  superseded_by INTEGER REFERENCES deliveries(id)
)'''


class RelayPersistence:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, VERSION):
                raise RuntimeError(f'无法打开存储版本 {version}')
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nCOMMIT;')
            self._migrate_columns(db)
            if version < 2:
                self._migrate_delivery_state(db)
            db.execute(f'PRAGMA user_version={VERSION}')

    @staticmethod
    def _columns(db, table):
        return {row[1] for row in db.execute(f'PRAGMA table_info({table})')}

    @classmethod
    def _migrate_columns(cls, db):
        subscriptions = cls._columns(db, 'subscriptions')
        if 'mode' not in subscriptions:
            db.execute("ALTER TABLE subscriptions ADD COLUMN mode TEXT NOT NULL DEFAULT 'all'")
        if 'mode_alias' not in subscriptions:
            db.execute("ALTER TABLE subscriptions ADD COLUMN mode_alias TEXT NOT NULL DEFAULT 'all'")
        if 'coalesce_timing' not in subscriptions:
            db.execute("ALTER TABLE subscriptions ADD COLUMN coalesce_timing TEXT NOT NULL DEFAULT 'ready'")

        deliveries = cls._columns(db, 'deliveries')
        if 'superseded_by' not in deliveries:
            db.execute('ALTER TABLE deliveries ADD COLUMN superseded_by INTEGER REFERENCES deliveries(id)')

    @classmethod
    def _migrate_delivery_state(cls, db):
        sql = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='deliveries'").fetchone()[0]
        if "'superseded'" in sql:
            return
        # The v1 CHECK constraint cannot be altered in place; rebuild the table.
        db.execute('PRAGMA foreign_keys=OFF')
        db.execute(DELIVERIES_V2)
        db.execute('''INSERT INTO deliveries_v2
                      (id,message_id,subscription_id,lane,policy,state,ready_at,attempt_count,
                       terminal_at,parent_id,superseded_by)
                      SELECT id,message_id,subscription_id,lane,policy,state,ready_at,attempt_count,
                             terminal_at,parent_id,NULL FROM deliveries''')
        db.execute('DROP TABLE deliveries')
        db.execute('ALTER TABLE deliveries_v2 RENAME TO deliveries')
        db.execute('''CREATE INDEX IF NOT EXISTS delivery_lane ON deliveries(subscription_id,lane,id)
                      WHERE state IN ('waiting','leased')''')
        db.execute('CREATE INDEX IF NOT EXISTS delivery_origin ON deliveries(message_id,id)')
        db.execute('PRAGMA foreign_keys=ON')

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
