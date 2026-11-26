VERSION = 1
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
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
  lane TEXT NOT NULL,
  policy INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('waiting','leased','acked','dead')),
  ready_at REAL NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  terminal_at REAL,
  parent_id INTEGER UNIQUE REFERENCES deliveries(id)
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
-- Publication forensics intentionally have no foreign key to messages and no
-- uniqueness on (channel, producer, dedup_key).  They must survive retention
-- and must not reserve a production key after the original message is pruned.
CREATE TABLE IF NOT EXISTS publication_audit_receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,
  message_id INTEGER NOT NULL,
  producer TEXT,
  dedup_key TEXT,
  generation INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL CHECK(status IN ('sealed','not_enabled','expired')),
  published_at REAL NOT NULL,
  recorded_at REAL NOT NULL,
  audit_until REAL,
  document TEXT NOT NULL,
  sha256 TEXT,
  UNIQUE(channel, message_id)
);
CREATE INDEX IF NOT EXISTS publication_audit_key
  ON publication_audit_receipts(channel, producer, dedup_key, id);
CREATE INDEX IF NOT EXISTS publication_audit_period
  ON publication_audit_receipts(channel, published_at, id);
CREATE TABLE IF NOT EXISTS audit_settings (
  channel TEXT PRIMARY KEY REFERENCES channels(name),
  enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
  retention_seconds INTEGER NOT NULL,
  enabled_at REAL,
  updated_at REAL NOT NULL
);
CREATE VIEW IF NOT EXISTS publication_audits AS
  SELECT * FROM publication_audit_receipts;
CREATE VIEW IF NOT EXISTS audit_receipts AS
  SELECT * FROM publication_audit_receipts;
CREATE VIEW IF NOT EXISTS publication_forensics AS
  SELECT * FROM publication_audit_receipts;
CREATE INDEX IF NOT EXISTS delivery_lane ON deliveries(subscription_id,lane,id)
  WHERE state IN ('waiting','leased');
CREATE INDEX IF NOT EXISTS delivery_origin ON deliveries(message_id,id);
CREATE INDEX IF NOT EXISTS attempt_deadline ON attempts(expires,id) WHERE outcome='open';
CREATE UNIQUE INDEX IF NOT EXISTS one_open_attempt ON attempts(delivery_id) WHERE outcome='open';
CREATE INDEX IF NOT EXISTS message_channel ON messages(channel,published,id);
CREATE INDEX IF NOT EXISTS journal_message ON journal(message_id,id);
'''
