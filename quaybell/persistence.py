import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schema import INDEXES, MIGRATION, SCHEMA, TRIGGER, VERSION


class RelayPersistence:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            prior = db.execute('PRAGMA user_version').fetchone()[0]
            if prior not in (0, 1, VERSION):
                raise RuntimeError(f'无法打开存储版本 {prior}')
            db.execute('PRAGMA journal_mode=WAL')
            steps = f'BEGIN IMMEDIATE;\n{SCHEMA}'
            if prior == 1:
                steps += f'\n{MIGRATION}'
            # 依赖 position 的索引和触发器在加列（若有）之后统一安装。
            steps += f'\n{INDEXES}\n{TRIGGER}\nPRAGMA user_version={VERSION};\nCOMMIT;'
            db.executescript(steps)

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
