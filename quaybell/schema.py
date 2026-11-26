VERSION = 2

# 普通发布和重投不显式给位置，由触发器在顺序键维度追加队尾；
# 补订前序和缺口屏障则在插入时显式指定位置，以便排在新发布之前。
TRIGGER = '''
CREATE TRIGGER IF NOT EXISTS delivery_position AFTER INSERT ON deliveries
FOR EACH ROW WHEN NEW.position IS NULL
BEGIN
  UPDATE deliveries SET position=COALESCE(
    (SELECT MAX(position)+1 FROM deliveries
     WHERE subscription_id=NEW.subscription_id AND lane=NEW.lane), 1)
  WHERE id=NEW.id;
END;
'''

SCHEMA = '''
CREATE TABLE IF NOT EXISTS channels (
  name TEXT PRIMARY KEY,
  created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS policies (
  channel TEXT NOT NULL REFERENCES channels(name),
  version INTEGER NOT NULL,
  lease_seconds INTEGER NOT NULL,
  retry_seconds INTEGER NOT NULL,
  max_attempts INTEGER NOT NULL,
  retention_seconds INTEGER NOT NULL,
  created REAL NOT NULL,
  PRIMARY KEY(channel, version)
);
CREATE TABLE IF NOT EXISTS subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL REFERENCES channels(name),
  name TEXT NOT NULL,
  tags TEXT NOT NULL,
  paused INTEGER NOT NULL DEFAULT 0 CHECK(paused IN (0,1)),
  created REAL NOT NULL,
  UNIQUE(channel, name)
);
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,
  producer TEXT NOT NULL,
  dedup_key TEXT NOT NULL,
  lane TEXT NOT NULL,
  envelope TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  policy INTEGER NOT NULL,
  published REAL NOT NULL,
  available REAL NOT NULL,
  FOREIGN KEY(channel, policy) REFERENCES policies(channel, version),
  UNIQUE(channel, producer, dedup_key)
);
CREATE TABLE IF NOT EXISTS deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
  subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
  lane TEXT NOT NULL,
  policy INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('waiting','leased','acked','dead','blocked')),
  ready_at REAL NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  terminal_at REAL,
  parent_id INTEGER UNIQUE REFERENCES deliveries(id),
  position INTEGER
);
CREATE TABLE IF NOT EXISTS attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id INTEGER NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  number INTEGER NOT NULL,
  token_digest TEXT NOT NULL UNIQUE,
  worker TEXT NOT NULL,
  started REAL NOT NULL,
  expires REAL NOT NULL,
  outcome TEXT NOT NULL CHECK('open' = outcome OR outcome IN ('acked','nacked','expired')),
  finished REAL,
  reason TEXT,
  receipt TEXT,
  UNIQUE(delivery_id, number)
);
CREATE TABLE IF NOT EXISTS journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL REFERENCES channels(name),
  message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
  at REAL NOT NULL,
  kind TEXT NOT NULL,
  details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backfills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL REFERENCES channels(name),
  subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
  name TEXT NOT NULL,
  tags TEXT NOT NULL,
  window_start REAL NOT NULL,
  window_end REAL NOT NULL,
  created_at REAL NOT NULL,
  UNIQUE(channel, subscription_id, name)
);
CREATE TABLE IF NOT EXISTS backfill_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  backfill_id INTEGER NOT NULL REFERENCES backfills(id) ON DELETE CASCADE,
  message_id INTEGER,
  delivery_id INTEGER UNIQUE REFERENCES deliveries(id),
  lane TEXT NOT NULL,
  position INTEGER,
  status TEXT NOT NULL CHECK(status IN
    ('scheduled','delivered','recovered_evicted','unclassified','evidence_pending',
     'accepted','skipped')),
  observed_at REAL,
  evidence_sha256 TEXT,
  resolution TEXT CHECK(resolution IS NULL OR resolution IN ('accepted','skipped')),
  resolved_at REAL,
  note TEXT
);
CREATE TABLE IF NOT EXISTS evidence_packs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL REFERENCES channels(name),
  message_id INTEGER NOT NULL,
  fingerprint TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  observed_at REAL NOT NULL,
  producer TEXT NOT NULL,
  dedup_key TEXT NOT NULL,
  lane TEXT NOT NULL,
  envelope TEXT NOT NULL,
  received_at REAL NOT NULL,
  backfill_id INTEGER REFERENCES backfills(id) ON DELETE CASCADE,
  UNIQUE(channel, message_id)
);
'''

# 依赖 deliveries.position 的索引必须在迁移加列之后创建，因此与建表脚本分开。
INDEXES = '''
CREATE INDEX IF NOT EXISTS delivery_lane ON deliveries(subscription_id,lane,position)
  WHERE state IN ('waiting','leased','blocked');
CREATE INDEX IF NOT EXISTS delivery_origin ON deliveries(message_id,id);
CREATE INDEX IF NOT EXISTS attempt_deadline ON attempts(expires,id) WHERE outcome='open';
CREATE UNIQUE INDEX IF NOT EXISTS one_open_attempt ON attempts(delivery_id) WHERE outcome='open';
CREATE INDEX IF NOT EXISTS message_channel ON messages(channel,published,id);
CREATE INDEX IF NOT EXISTS journal_message ON journal(message_id,id);
CREATE INDEX IF NOT EXISTS backfill_lookup ON backfill_entries(backfill_id,status);
CREATE INDEX IF NOT EXISTS evidence_message ON evidence_packs(channel,message_id);
'''

# 旧库（VERSION=1）的 deliveries 没有逻辑位置列，按既有投递 ID 回填，
# 之后统一由触发器在顺序键维度追加位置。
MIGRATION = '''
ALTER TABLE deliveries ADD COLUMN position INTEGER;
UPDATE deliveries SET position=id WHERE position IS NULL;
'''
