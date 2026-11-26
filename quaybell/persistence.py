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
  state TEXT NOT NULL CHECK(state IN ('waiting','leased','acked','dead','transferred')),
  ready_at REAL NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  terminal_at REAL,
  parent_id INTEGER UNIQUE REFERENCES deliveries(id)
)'''

ATTEMPTS_V2 = '''CREATE TABLE attempts_v2 (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id INTEGER NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  number INTEGER NOT NULL,
  token_digest TEXT NOT NULL UNIQUE,
  worker TEXT NOT NULL,
  started REAL NOT NULL,
  expires REAL NOT NULL,
  outcome TEXT NOT NULL CHECK('open' = outcome OR outcome IN ('acked','nacked','expired','superseded')),
  finished REAL,
  reason TEXT,
  receipt TEXT,
  UNIQUE(delivery_id, number)
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
                self._migrate_v2(db)
            db.execute(f'PRAGMA user_version={VERSION}')

    @staticmethod
    def _migrate_columns(db):
        columns = {row[1] for row in db.execute('PRAGMA table_info(group_takeovers)')}
        if columns and 'strategy' not in columns:
            db.execute("ALTER TABLE group_takeovers ADD COLUMN strategy TEXT NOT NULL DEFAULT 'copy'")

    @classmethod
    def _table_sql(cls, db, table):
        row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                        (table,)).fetchone()
        return row[0] if row else ''

    @classmethod
    def _rebuild_deliveries(cls, db):
        sql = cls._table_sql(db, 'deliveries')
        if "'transferred'" in sql:
            return
        db.execute('PRAGMA foreign_keys=OFF')
        db.execute(DELIVERIES_V2)
        db.execute('''INSERT INTO deliveries_v2
                      (id,message_id,subscription_id,lane,policy,state,ready_at,attempt_count,
                       terminal_at,parent_id)
                      SELECT id,message_id,subscription_id,lane,policy,state,ready_at,attempt_count,
                             terminal_at,parent_id FROM deliveries''')
        db.execute('DROP TABLE deliveries')
        db.execute('ALTER TABLE deliveries_v2 RENAME TO deliveries')
        db.execute('''CREATE INDEX IF NOT EXISTS delivery_lane ON deliveries(subscription_id,lane,id)
                      WHERE state IN ('waiting','leased')''')
        db.execute('CREATE INDEX IF NOT EXISTS delivery_origin ON deliveries(message_id,id)')
        db.execute('PRAGMA foreign_keys=ON')

    @classmethod
    def _rebuild_attempts(cls, db):
        sql = cls._table_sql(db, 'attempts')
        if "'superseded'" in sql:
            return
        db.execute('PRAGMA foreign_keys=OFF')
        db.execute(ATTEMPTS_V2)
        db.execute('''INSERT INTO attempts_v2
                      (id,delivery_id,number,token_digest,worker,started,expires,outcome,
                       finished,reason,receipt)
                      SELECT id,delivery_id,number,token_digest,worker,started,expires,outcome,
                             finished,reason,receipt FROM attempts''')
        db.execute('DROP TABLE attempts')
        db.execute('ALTER TABLE attempts_v2 RENAME TO attempts')
        db.execute('''CREATE INDEX IF NOT EXISTS attempt_deadline
                      ON attempts(expires,id) WHERE outcome='open' ''')
        db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS one_open_attempt
                      ON attempts(delivery_id) WHERE outcome='open' ''')
        db.execute('PRAGMA foreign_keys=ON')

    @classmethod
    def _migrate_v2(cls, db):
        # SQLite cannot enlarge a CHECK constraint in place. Rebuild the two
        # tables while preserving ids so takeover and attempt history stay stable.
        cls._rebuild_deliveries(db)
        cls._rebuild_attempts(db)

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
