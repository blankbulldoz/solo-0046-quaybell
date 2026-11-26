VERSION = 2
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
CREATE INDEX IF NOT EXISTS delivery_lane ON deliveries(subscription_id,lane,id)
  WHERE state IN ('waiting','leased');
CREATE INDEX IF NOT EXISTS delivery_origin ON deliveries(message_id,id);
CREATE INDEX IF NOT EXISTS attempt_deadline ON attempts(expires,id) WHERE outcome='open';
CREATE UNIQUE INDEX IF NOT EXISTS one_open_attempt ON attempts(delivery_id) WHERE outcome='open';
CREATE INDEX IF NOT EXISTS message_channel ON messages(channel,published,id);
CREATE INDEX IF NOT EXISTS journal_message ON journal(message_id,id);
CREATE TABLE IF NOT EXISTS receipt_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_key TEXT NOT NULL UNIQUE,
  worker TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS receipt_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL REFERENCES receipt_batches(id),
  position INTEGER NOT NULL,
  delivery_id INTEGER NOT NULL,
  disposition TEXT NOT NULL CHECK(disposition IN ('acked','nacked')),
  token_digest TEXT NOT NULL,
  reason TEXT,
  status TEXT NOT NULL CHECK(status IN ('applied','replayed','stale','conflict')),
  detail TEXT NOT NULL,
  result TEXT,
  UNIQUE(batch_id, position)
);
CREATE INDEX IF NOT EXISTS receipt_entry_batch ON receipt_entries(batch_id,position);
'''
