import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import MIGRATIONS, SCHEMA, VERSION


class RelayPersistence:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            db.execute('PRAGMA journal_mode=WAL')
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, VERSION) and version not in MIGRATIONS:
                raise RuntimeError(f'无法打开存储版本 {version}')
            if version in MIGRATIONS:
                self._migrate(db, version)
            db.executescript(f'BEGIN IMMEDIATE;\n{SCHEMA}\nPRAGMA user_version={VERSION};\nCOMMIT;')

    @staticmethod
    def _migrate(db, version):
        # Rebuilding a parent table must not cascade into deliveries, attempts or
        # journal rows, so foreign keys stay off for the whole script.
        db.execute('PRAGMA foreign_keys=OFF')
        db.executescript(f'BEGIN IMMEDIATE;\n{MIGRATIONS[version]}\nPRAGMA user_version={version + 1};\nCOMMIT;')
        if db.execute('PRAGMA foreign_key_check').fetchone() is not None:
            raise RuntimeError(f'存储版本 {version} 迁移后外键不一致')
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
