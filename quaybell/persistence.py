import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import SCHEMA, VERSION

# 旧版本库到当前版本的在线迁移：键为该库当前 user_version，值为升到下一版的语句。
# 全新库由 SCHEMA 直接建成最新形态（user_version=0），不重放任何迁移。
MIGRATIONS = {
    1: [
        # 每个消费组的发布额度，0 表示不限；旧库的既有积压不被新额度追溯。
        'ALTER TABLE subscriptions ADD COLUMN publish_quota INTEGER NOT NULL DEFAULT 0;',
        'CREATE INDEX IF NOT EXISTS delivery_outstanding ON deliveries(subscription_id) '
        "WHERE state IN ('waiting','leased');",
    ],
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
            # 只有真实存在的旧版本库（user_version>=1）才迁移；全新库已是最新 SCHEMA。
            migration = '\n'.join(statement for step in range(version, VERSION)
                                  for statement in MIGRATIONS.get(step, ())) if version else ''
            db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\n{migration}\n'
                             f'PRAGMA user_version={VERSION};\nCOMMIT;')

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
