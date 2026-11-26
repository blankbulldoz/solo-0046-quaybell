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
  state TEXT NOT NULL CHECK(state IN ('waiting','leased','acked','dead','transferred')),
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
  outcome TEXT NOT NULL CHECK('open' = outcome OR outcome IN ('acked','nacked','expired','superseded')),
  finished REAL,
  reason TEXT,
  receipt TEXT,
  UNIQUE(delivery_id, number)
);
CREATE TABLE IF NOT EXISTS group_takeovers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL REFERENCES channels(name),
  source_group_id INTEGER NOT NULL REFERENCES subscriptions(id),
  target_group_id INTEGER NOT NULL REFERENCES subscriptions(id),
  client_key TEXT,
  strategy TEXT NOT NULL DEFAULT 'copy' CHECK(strategy IN ('copy','move')),
  status TEXT NOT NULL DEFAULT 'completed' CHECK(status IN ('completed')),
  terminate_leases INTEGER NOT NULL DEFAULT 1 CHECK(terminate_leases IN (0,1)),
  pause_source INTEGER NOT NULL DEFAULT 0 CHECK(pause_source IN (0,1)),
  request TEXT NOT NULL,
  summary TEXT NOT NULL DEFAULT '{}',
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  completed_at REAL NOT NULL,
  last_retry_at REAL,
  UNIQUE(channel, client_key)
);
CREATE TABLE IF NOT EXISTS takeover_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  takeover_id INTEGER NOT NULL REFERENCES group_takeovers(id) ON DELETE CASCADE,
  channel TEXT NOT NULL REFERENCES channels(name),
  lane TEXT NOT NULL,
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  source_delivery_id INTEGER NOT NULL UNIQUE REFERENCES deliveries(id),
  target_delivery_id INTEGER NOT NULL UNIQUE REFERENCES deliveries(id),
  before_state TEXT NOT NULL,
  lease_attempt INTEGER,
  lease_expires REAL,
  terminated_at REAL,
  created_at REAL NOT NULL
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
CREATE UNIQUE INDEX IF NOT EXISTS takeover_client_key
  ON group_takeovers(channel, client_key) WHERE client_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS takeover_pair
  ON group_takeovers(channel,source_group_id,target_group_id,id);
CREATE INDEX IF NOT EXISTS takeover_items_takeover ON takeover_items(takeover_id,id);
'''
