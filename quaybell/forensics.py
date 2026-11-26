"""Immutable publication routing evidence and audit-period queries.

The records in this module are deliberately separate from ``messages``.  They
do not have a production-key uniqueness constraint: retaining evidence about an
old receipt must never prevent publishing a new envelope under that key after
ordinary retention has deleted the message row.
"""

from . import catalog, codec, inputs, subscriptions
from .errors import missing

FORMAT = 'quaybell-publication-audit/1'
DEFAULT_RETENTION_SECONDS = 90 * 24 * 60 * 60
MIN_RETENTION_SECONDS = 0
MAX_RETENTION_SECONDS = 10 * 366 * 24 * 60 * 60
TTL_FIELDS = ('audit_retention_seconds', 'audit_period_seconds',
              'audit_ttl_seconds', 'audit_seconds')
ENABLE_FIELDS = ('audit_enabled', 'audit_credential_enabled',
                 'credential_enabled', 'forensics_enabled')
TIME_FIELDS = ('start', 'from', 'since', 'begin', 'period_start',
               'start_at', 'from_at', 'audit_start', 'period_start_at')
END_FIELDS = ('end', 'to', 'until', 'period_end',
              'end_at', 'to_at', 'audit_end', 'period_end_at')
KEY_FIELDS = ('key', 'dedup_key', 'production_key')


def ttl_from(raw, default=DEFAULT_RETENTION_SECONDS):
    for field in TTL_FIELDS:
        if field in raw:
            return inputs.integer(raw[field], MIN_RETENTION_SECONDS,
                                  MAX_RETENTION_SECONDS, '审计期秒数')
    if isinstance(raw.get('audit'), dict):
        return ttl_from(raw['audit'], default)
    return default


def enabled_from(raw, default=True):
    for field in ENABLE_FIELDS:
        if field in raw:
            return inputs.boolean(raw[field], '审计凭证开关')
    audit = raw.get('audit')
    if isinstance(audit, dict) and 'enabled' in audit:
        return inputs.boolean(audit['enabled'], '审计凭证开关')
    if 'enabled' in raw and _looks_like_audit_config(raw):
        return inputs.boolean(raw['enabled'], '审计凭证开关')
    return default


def _looks_like_audit_config(raw):
    return any(field in raw for field in ('retention_seconds',) + TTL_FIELDS) or \
        set(raw) <= {'enabled', 'retention_seconds'}


def configure(db, channel, raw, now):
    catalog.get(db, channel)
    raw = dict(raw or {})
    for field in ENABLE_FIELDS:
        if field in raw and 'enabled' not in raw:
            raw['enabled'] = raw[field]
    if isinstance(raw.get('audit'), bool):
        raw['enabled'] = raw['audit']
        raw.pop('audit')
    elif isinstance(raw.get('audit'), dict):
        nested = raw.pop('audit')
        raw = nested | {key: value for key, value in raw.items() if key != 'audit'}
    inputs.require(raw, (), ('enabled', 'retention_seconds', *TTL_FIELDS, *ENABLE_FIELDS))
    row = db.execute('SELECT * FROM audit_settings WHERE channel=?', (channel,)).fetchone()
    old_enabled = bool(row['enabled']) if row else True
    old_ttl = row['retention_seconds'] if row else DEFAULT_RETENTION_SECONDS
    enabled = inputs.boolean(raw['enabled'], '审计凭证开关') if 'enabled' in raw else old_enabled
    ttl = inputs.integer(raw['retention_seconds'], MIN_RETENTION_SECONDS,
                         MAX_RETENTION_SECONDS, '审计期秒数') \
        if 'retention_seconds' in raw else ttl_from(raw, old_ttl)
    if ttl == 0:
        enabled = False
    if row is None:
        enabled_at = now if enabled else None
    elif enabled and not old_enabled:
        enabled_at = now
    else:
        enabled_at = row['enabled_at']
    db.execute('''INSERT INTO audit_settings(channel,enabled,retention_seconds,enabled_at,updated_at)
                  VALUES (?,?,?,?,?)
                  ON CONFLICT(channel) DO UPDATE SET
                    enabled=excluded.enabled,
                    retention_seconds=excluded.retention_seconds,
                    enabled_at=excluded.enabled_at,
                    updated_at=excluded.updated_at''',
               (channel, int(enabled), ttl, enabled_at, now))
    return settings(db, channel)


def settings(db, channel):
    row = db.execute('SELECT * FROM audit_settings WHERE channel=?', (channel,)).fetchone()
    if row is None:
        return {'enabled': True, 'retention_seconds': DEFAULT_RETENTION_SECONDS,
                'enabled_at': None, 'configured': False}
    return {'enabled': bool(row['enabled']), 'retention_seconds': row['retention_seconds'],
            'enabled_at': row['enabled_at'], 'updated_at': row['updated_at'],
            'configured': True}


def settings_from_creation(raw, now):
    """Read optional creation settings without letting policy parsing see them."""
    audit_raw = raw.get('audit') if isinstance(raw.get('audit'), dict) else {}
    merged = dict(audit_raw)
    for field in ('audit_enabled', *TTL_FIELDS):
        if field in raw:
            merged[field] = raw[field]
    if 'audit_retention_seconds' in raw:
        merged.setdefault('retention_seconds', raw['audit_retention_seconds'])
    elif any(field in raw for field in TTL_FIELDS):
        merged.setdefault('retention_seconds', ttl_from(raw))
    enabled = enabled_from(raw, enabled_from(audit_raw, True))
    if 'retention_seconds' in raw:
        ttl = inputs.integer(raw['retention_seconds'], MIN_RETENTION_SECONDS,
                             MAX_RETENTION_SECONDS, '审计期秒数')
    else:
        ttl = ttl_from(raw, ttl_from(audit_raw, DEFAULT_RETENTION_SECONDS))
    if ttl == 0:
        enabled = False
    return enabled, ttl, (now if enabled else None)


def _condition(group, key, expected, tags):
    actual = tags.get(key)
    return {'key': key, 'expected': expected, 'actual': actual,
            'matches': actual == expected}


def routing_basis(db, channel, tags):
    groups = subscriptions.for_channel(db, channel)
    captured = []
    matched = []
    for group in groups:
        conditions = [_condition(group, key, value, tags)
                      for key, value in group['tags'].items()]
        is_match = all(item['matches'] for item in conditions)
        if is_match:
            matched.append(group['name'])
        captured.append({'subscription_id': group['id'], 'group': group['name'],
                         'tags': group['tags'], 'filter': group['tags'],
                         'empty_filter': not group['tags'], 'paused': group['paused'],
                         'matched': is_match, 'conditions': conditions})
    return {'match_semantics': '组筛选中的每个键都必须与信封标签精确相等；空筛选接收全部消息',
            'envelope_tags': tags, 'groups_at_publication': captured,
            'groups': captured, 'matched_groups': matched,
            'matched': matched, 'subscription_ids': [group['subscription_id'] for group in captured]}


def _next_generation(db, channel, producer, key):
    row = db.execute('''SELECT COUNT(*) AS n FROM publication_audit_receipts
                        WHERE channel=? AND producer=? AND dedup_key=?''',
                     (channel, producer, key)).fetchone()
    return row['n'] + 1


def publish_override(raw, now):
    """Read optional per-publication audit-period overrides."""
    if not isinstance(raw, dict):
        return None
    candidate = dict(raw.get('audit')) if isinstance(raw.get('audit'), dict) else {}
    for field in (*ENABLE_FIELDS, *TTL_FIELDS, 'audit_until', 'audit_period', 'ttl'):
        if field in raw:
            candidate[field] = raw[field]
    if not candidate:
        return None
    result = {}
    for field in ENABLE_FIELDS:
        if field in candidate:
            result['enabled'] = inputs.boolean(candidate[field], '审计凭证开关')
            break
    if 'enabled' in candidate:
        result['enabled'] = inputs.boolean(candidate['enabled'], '审计凭证开关')
    ttl = None
    for field in ('retention_seconds', 'ttl', 'audit_period', *TTL_FIELDS):
        if field in candidate:
            ttl = inputs.integer(candidate[field], 0, MAX_RETENTION_SECONDS, '审计期秒数')
            break
    if 'audit_until' in candidate or 'until' in candidate:
        until = candidate.get('audit_until', candidate.get('until'))
        if isinstance(until, str) and until.replace('.', '', 1).isdecimal():
            until = float(until)
        if isinstance(until, (int, float)) and not isinstance(until, bool):
            if until <= now:
                raise inputs.invalid('审计截止时间须晚于当前时间')
            ttl = float(until) - now
    if ttl is not None:
        result['retention_seconds'] = ttl
        if ttl == 0:
            result['enabled'] = False
    return result


def record_publication(db, channel, message, envelope, receipt, now, override=None):
    """Persist the original receipt and rule snapshot in the publish transaction."""
    existing = db.execute('SELECT id FROM publication_audit_receipts WHERE channel=? AND message_id=?',
                          (channel, message['id'])).fetchone()
    if existing is not None:
        return existing['id']
    config = settings(db, channel)
    if override:
        config = config | {key: value for key, value in override.items() if value is not None}
        if config.get('retention_seconds', DEFAULT_RETENTION_SECONDS) == 0:
            config['enabled'] = False
    generation = _next_generation(db, channel, envelope.producer, envelope.dedup_key)
    published_at = message['published'] if isinstance(message, dict) and 'published' in message else now
    if not config['enabled']:
        document = {
            'format': FORMAT,
            'kind': 'audit_credential_not_enabled',
            'channel': channel,
            'message_id': message['id'],
            'producer': envelope.producer,
            'key': envelope.dedup_key,
            'dedup_key': envelope.dedup_key,
            'lane': envelope.lane,
            'published_at': published_at,
            'status': 'audit_credential_not_enabled',
            'provable_facts': {
                'channel': channel, 'message_id': message['id'],
                'producer': envelope.producer, 'key': envelope.dedup_key,
                'lane': envelope.lane, 'published_at': published_at,
            },
            'unavailable_evidence': [
                'envelope_tags', 'payload_digest', 'group_rules_at_publication',
                'original_fanout_receipt',
            ],
            'statement': '该历史发布发生时审计凭证尚未启用；不能用当前拓扑反推当时标签或组规则。',
        }
        encoded = codec.pack(document)
        audit_id = db.execute('''INSERT INTO publication_audit_receipts
            (channel,message_id,producer,dedup_key,generation,status,published_at,recorded_at,
             audit_until,document,sha256)
            VALUES (?,?,?,?,?,'not_enabled',?,?,NULL,?,NULL)''',
            (channel, message['id'], envelope.producer, envelope.dedup_key, generation,
             published_at, now, encoded)).lastrowid
        return audit_id

    basis = routing_basis(db, channel, envelope.tags)
    envelope_document = {'producer': envelope.producer, 'key': envelope.dedup_key,
                         'dedup_key': envelope.dedup_key, 'lane': envelope.lane,
                         'tags': envelope.tags, 'payload': envelope.payload,
                         'delay_seconds': envelope.delay_seconds,
                         'canonical_envelope': codec.unpack(envelope.encoded()),
                         'sha256': codec.fingerprint(envelope.encoded())}
    document = {
        'format': FORMAT,
        'kind': 'original_publication_receipt',
        'channel': channel,
        'message_id': message['id'],
        'producer': envelope.producer,
        'key': envelope.dedup_key,
        'dedup_key': envelope.dedup_key,
        'lane': envelope.lane,
        'published_at': published_at,
        'policy_version': message['policy'],
        'envelope': envelope_document,
        'tags': envelope.tags,
        'original_receipt': receipt,
        'receipt': receipt,
        'routing_basis': basis,
        'audit_credential': {
            'status': 'enabled',
            'format': FORMAT,
            'recorded_at': now,
            'enabled_at': config.get('enabled_at'),
            'retention_seconds': config['retention_seconds'],
            'expires_at': now + config['retention_seconds'],
        },
    }
    digest = codec.fingerprint(codec.pack(document))
    document['sha256'] = digest
    encoded = codec.pack(document)
    audit_id = db.execute('''INSERT INTO publication_audit_receipts
        (channel,message_id,producer,dedup_key,generation,status,published_at,recorded_at,
         audit_until,document,sha256)
        VALUES (?,?,?,?,?,'sealed',?,?,?,?,?)''',
        (channel, message['id'], envelope.producer, envelope.dedup_key, generation,
         published_at, now, now + config['retention_seconds'], encoded, digest)).lastrowid
    return audit_id


def _stored_document(row):
    document = codec.unpack(row['document'])
    digest = row['sha256']
    recomputed = codec.fingerprint(codec.pack(document))
    # Records written before sha256 was inserted into the document still verify
    # against the canonical document stored in the row.
    if 'sha256' in document and document['sha256'] == digest:
        body = dict(document)
        body_digest = body.pop('sha256')
        matches = body_digest == digest and codec.fingerprint(codec.pack(body)) == digest
    else:
        matches = recomputed == digest
    return document, matches


def _prune_fact(db, message_id, channel=None):
    sql = "SELECT id,channel,message_id,at,details FROM journal WHERE kind='pruned'"
    params = []
    if channel is not None:
        sql += ' AND channel=?'
        params.append(channel)
    sql += ' ORDER BY id DESC'
    for row in db.execute(sql, params):
        try:
            details = codec.unpack(row['details'])
        except ValueError:
            continue
        if message_id in details.get('message_ids', []):
            return {'journal_id': row['id'], 'channel': row['channel'],
                    'pruned_at': row['at'], 'removed_message_ids': details.get('message_ids', []),
                    'message_level_audit_deleted': True}
    return None


def _current_topology(db, channel, tags, known_ids, published_at):
    groups = subscriptions.for_channel(db, channel)
    current_matches = []
    current_groups = []
    later = []
    for group in groups:
        matched = all(tags.get(key) == value for key, value in group['tags'].items())
        if matched:
            current_matches.append(group['name'])
        existed = group['id'] in (known_ids or set())
        item = {'subscription_id': group['id'], 'group': group['name'], 'tags': group['tags'],
                'matched_now': matched, 'existed_at_publication': existed}
        current_groups.append(item)
        if not existed:
            later.append(group['name'])
    return {'current_preview_groups': current_matches,
            'current_preview_matches': current_matches,
            'groups_created_after_publication': later,
            'groups': current_groups,
            'would_include_later_groups': bool(later and any(
                item['matched_now'] and not item['existed_at_publication'] for item in current_groups)),
            'warning': '当前预查读取最新拓扑，不能替代发布时冻结的组规则依据。'}


def _live_report(db, message_id, now, channel=None, reason='audit_credential_not_enabled'):
    from . import publication
    row = db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
    if row is None or (channel is not None and row['channel'] != channel):
        return None
    message = publication.get(db, message_id)
    receipt = publication.receipt(db, row, False)
    tags = message['envelope'].get('tags', {})
    basis = routing_basis(db, row['channel'], tags)
    # Routing basis above uses current subscription rows.  Mark every group as
    # reconstructed, not as a sealed historical rule snapshot.
    for group in basis['groups_at_publication']:
        group['reconstructed_from_live_topology'] = True
    prune = None
    return {
        'audit_id': None, 'id': None, 'channel': row['channel'], 'message_id': message_id,
        'producer': row['producer'], 'key': row['dedup_key'], 'dedup_key': row['dedup_key'],
        'generation': None, 'published_at': row['published'], 'observed_at': now,
        'status': reason, 'credential_status': 'not_enabled',
        'audit_credential': {'status': 'not_enabled', 'enabled': False},
        'audit_enabled': False, 'credential_enabled': False,
        'evidence_status': 'live_reconstruction',
        'available': True, 'certified': False, 'provable': True,
        'routing_provable': True, 'can_reconstruct_routing': True, 'message_present': True,
        'original_receipt': receipt, 'receipt': receipt,
        'tags': tags, 'labels': tags, 'routing_basis': basis, 'basis': basis,
        'rules': basis.get('groups_at_publication', []),
        'current_topology': _current_topology(db, row['channel'], tags,
                                              set(basis['subscription_ids']), row['published']),
        'prune_record': prune,
        'provable_facts': {'message_row_present': True, 'original_fanout_rows_present': True,
                           'publication_receipt': receipt},
        'unavailable_evidence': ['sealed_audit_credential'],
        'limitations': ['审计凭证未在发布时启用；以下依据来自仍在库的主记录，不是发布时封存的凭证。'],
    }


def view(db, row, now, *, include_current=True):
    document, digest_matches = _stored_document(row)
    channel = row['channel']
    message_present = db.execute('SELECT 1 FROM messages WHERE id=?', (row['message_id'],)).fetchone() is not None
    prune = None if message_present else _prune_fact(db, row['message_id'], channel)
    expired = row['status'] == 'expired' or (row['audit_until'] is not None and now >= row['audit_until'])
    tags = document.get('tags')
    known_ids = set((document.get('routing_basis') or {}).get('subscription_ids', []))
    current = _current_topology(db, channel, tags or {}, known_ids, row['published_at']) \
        if include_current and tags is not None else None
    base = {
        'audit_id': row['id'], 'id': row['id'], 'channel': channel,
        'message_id': row['message_id'], 'producer': row['producer'],
        'key': row['dedup_key'], 'dedup_key': row['dedup_key'],
        'generation': row['generation'], 'published_at': row['published_at'],
        'recorded_at': row['recorded_at'], 'audit_until': row['audit_until'],
        'observed_at': now, 'sha256': row['sha256'], 'digest_matches': digest_matches,
        'message_present': message_present, 'prune_record': prune,
        'current_topology': current,
    }
    if row['status'] == 'not_enabled':
        live_report = _live_report(db, row['message_id'], now, channel) if message_present else None
        credential = {'status': 'not_enabled', 'enabled': False,
                      'reason': 'audit_credential_was_not_enabled'}
        return base | {
            'status': 'audit_credential_not_enabled', 'credential_status': 'not_enabled',
            'audit_credential': credential, 'audit_enabled': False, 'credential_enabled': False,
            'evidence_status': 'unavailable', 'available': False, 'certified': False,
            'live_available': live_report is not None,
            'live_reconstruction': live_report,
            'provable': True, 'routing_provable': live_report is not None,
            'can_reconstruct_routing': live_report is not None,
            'original_receipt': None, 'receipt': None, 'tags': None, 'labels': None,
            'routing_basis': None, 'basis': None, 'rules': None,
            'provable_facts': document.get('provable_facts', {}) | {
                'coverage_marker_recorded': True,
                'message_present': message_present,
                'prune_record': prune,
            },
            'unavailable_evidence': document.get('unavailable_evidence', []),
            'limitations': [document.get('statement')] + (
                ['原消息仍在库；live_reconstruction 是当前主记录重建，不是封存凭证。']
                if live_report is not None else []),
            'document': document,
        }
    if expired:
        credential = {'status': 'expired', 'enabled': False,
                      'audit_until': row['audit_until']}
        return base | {
            'status': 'audit_period_expired', 'credential_status': 'expired',
            'audit_credential': credential, 'audit_enabled': False, 'credential_enabled': False,
            'evidence_status': 'expired', 'available': False, 'certified': False,
            'provable': False, 'routing_provable': False, 'can_reconstruct_routing': False,
            'original_receipt': None, 'receipt': None, 'tags': None, 'labels': None,
            'routing_basis': None, 'basis': None, 'rules': None,
            'provable_facts': {'audit_id': row['id'], 'message_id': row['message_id'],
                               'published_at': row['published_at'], 'audit_until': row['audit_until'],
                               'prune_record': prune},
            'unavailable_evidence': ['sealed_routing_basis_after_audit_period'],
            'limitations': ['已超过指定审计期；封存内容不再作为可读取证据。'],
        }
    receipt = document.get('original_receipt')
    basis = document.get('routing_basis')
    return base | {
        'status': 'verified', 'credential_status': 'enabled', 'evidence_status': 'proven',
        'audit_credential': (document.get('audit_credential') or {'status': 'enabled'}) | {'enabled': True},
        'audit_enabled': True, 'credential_enabled': True,
        'available': True, 'certified': True, 'provable': True, 'routing_provable': True,
        'can_reconstruct_routing': True,
        'original_receipt': receipt, 'receipt': receipt,
        'fanout': (receipt or {}).get('fanout', {}),
        'matched_groups': (basis or {}).get('matched_groups', []),
        'groups': (basis or {}).get('groups_at_publication', []),
        'tags': tags, 'labels': tags,
        'routing_basis': basis, 'basis': basis,
        'rules': (basis or {}).get('groups_at_publication', []),
        'provable_facts': {
            'original_receipt': receipt, 'tags_at_publication': tags,
            'group_rules_at_publication': (basis or {}).get('groups_at_publication', []),
            'digest_matches': digest_matches,
        },
        'unavailable_evidence': [],
        'limitations': [] if digest_matches else ['审计凭证摘要校验失败，记录可能已损坏。'],
        'document': document,
    }


def _with_aliases(item):
    item.setdefault('state', item.get('status'))
    item.setdefault('reason', item.get('status'))
    item.setdefault('reason_code', item.get('status'))
    item.setdefault('code', item.get('status'))
    return item


def for_message(db, message_id, now, channel=None):
    row = db.execute('''SELECT * FROM publication_audit_receipts
                        WHERE message_id=? AND (? IS NULL OR channel=?)
                        ORDER BY id DESC LIMIT 1''',
                     (message_id, channel, channel)).fetchone()
    if row is not None:
        return _with_aliases(view(db, row, now))
    live = _live_report(db, message_id, now, channel)
    if live is not None:
        return _with_aliases(live)
    prune = _prune_fact(db, message_id, channel)
    if prune is not None:
        report = {
            'audit_id': None, 'id': None, 'channel': prune['channel'], 'message_id': message_id,
            'observed_at': now, 'status': 'audit_credential_not_enabled',
            'credential_status': 'not_enabled',
            'audit_credential': {'status': 'not_enabled', 'enabled': False},
            'audit_enabled': False, 'credential_enabled': False,
            'evidence_status': 'unavailable',
            'available': False, 'certified': False, 'provable': True, 'routing_provable': False,
            'can_reconstruct_routing': False,
            'message_present': False, 'prune_record': prune,
            'provable_facts': {'message_id': message_id, 'channel_prune_record': prune},
            'unavailable_evidence': ['tags_at_publication', 'group_rules_at_publication',
                                     'original_receipt'],
            'tags': None, 'labels': None, 'routing_basis': None, 'basis': None, 'rules': None,
            'original_receipt': None, 'receipt': None,
            'limitations': ['该历史记录的审计凭证尚未启用；没有发布时封存的标签和规则，只能证明频道级清理记录中的 ID。'],
        }
        return _with_aliases(report)
    raise missing('找不到该发布的审计记录或可证明的清理痕迹')


def for_credential(db, audit_id, now):
    row = db.execute('SELECT * FROM publication_audit_receipts WHERE id=?', (audit_id,)).fetchone()
    if row is None:
        # Allow callers to use the original message id as the credential locator.
        return for_message(db, audit_id, now)
    return _with_aliases(view(db, row, now))


def original_receipt(db, message_id, now, channel=None):
    audit = for_message(db, message_id, now, channel)
    receipt = audit.get('original_receipt')
    if receipt is None:
        raise missing('原回执没有可读取的封存副本')
    return receipt


def _number(raw, fields, label):
    for field in fields:
        if field in raw and raw[field] is not None:
            value = raw[field]
            if isinstance(value, str):
                try:
                    value = float(value)
                except ValueError:
                    raise inputs.invalid(f'{label}须为 Unix 秒数值') from None
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise inputs.invalid(f'{label}须为 Unix 秒数值')
            return float(value)
    return None


def _integer_param(raw, field, label, low=1, high=10**18):
    value = raw.get(field)
    if value is None:
        return None
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    return inputs.integer(value, low, high, label)


def query(db, channel, raw, now):
    raw = raw or {}
    catalog.get(db, channel)
    message_id = _integer_param(raw, 'message_id', '消息号') or raw.get('id')
    if message_id is not None and not isinstance(message_id, int):
        message_id = _integer_param({'id': message_id}, 'id', '消息号')
    if message_id is not None:
        return for_message(db, message_id, now, channel)
    audit_id = _integer_param(raw, 'audit_id', '审计凭证号') \
        or _integer_param(raw, 'receipt_id', '回执号') \
        or _integer_param(raw, 'credential_id', '审计凭证号')
    if audit_id is not None:
        item = for_credential(db, audit_id, now)
        if item['channel'] != channel:
            raise missing('找不到该频道下的审计凭证')
        return item

    start = _number(raw, TIME_FIELDS, '审计期起点')
    end = _number(raw, END_FIELDS, '审计期终点')
    period = raw.get('audit_period', raw.get('period', raw.get('range', raw.get('window'))))
    if isinstance(period, dict):
        start = start if start is not None else _number(period, ('start', 'from', 'since'), '审计期起点')
        end = end if end is not None else _number(period, ('end', 'to', 'until'), '审计期终点')
    elif isinstance(period, str):
        try:
            period_seconds = float(period)
        except ValueError:
            period_seconds = None
        if period_seconds is not None and start is None:
            start = now - period_seconds
            end = now
    elif isinstance(period, (int, float)) and not isinstance(period, bool) and start is None:
        start = now - float(period)
        end = now
    if start is not None and end is not None and start > end:
        raise inputs.invalid('审计期起点不能晚于终点')

    sql = 'SELECT * FROM publication_audit_receipts WHERE channel=?'
    params = [channel]
    producer = raw.get('producer', raw.get('producer_id'))
    if producer is not None:
        sql += ' AND producer=?'
        params.append(inputs.token(producer, '生产者'))
    key = next((raw[field] for field in KEY_FIELDS if field in raw), None)
    if key is not None:
        sql += ' AND dedup_key=?'
        params.append(inputs.token(key, '生产键'))
    if start is not None:
        sql += ' AND published_at>=?'
        params.append(start)
    if end is not None:
        sql += ' AND published_at<=?'
        params.append(end)
    limit = _integer_param(raw, 'limit', '审计记录上限', 1, 500) or 100
    sql += ' ORDER BY id DESC LIMIT ?'
    params.append(limit)
    rows = db.execute(sql, params).fetchall()
    records = [_with_aliases(view(db, row, now)) for row in rows]
    # Key queries are lifecycle questions; answer in chronological order.
    if key is not None and raw.get('order', 'asc') != 'desc':
        records.reverse()
    unavailable = [item for item in records if not item.get('available')]
    result = {
        'channel': channel, 'observed_at': now,
        'period': {'start': start, 'end': end} if start is not None or end is not None else None,
        'count': len(records), 'records': records, 'publications': records,
        'items': records, 'unavailable': unavailable,
        'production_key_permanently_reserved': False,
        'key_unique_in_audit_table': False,
        'evidence_boundary': {
            'original_receipt_is_authoritative': True,
            'current_preview_is_historical_proof': False,
            'audit_records_do_not_lock_production_keys': True,
            'unavailable_records_are_marked_not_reconstructed': True,
        },
    }
    if key is not None:
        result['production_key'] = key
        result['generations'] = records
        result['oldest'] = records[0] if records else None
        result['latest'] = records[-1] if records else None
    return result


def expire_due(db, channel, now):
    rows = db.execute('''SELECT id FROM publication_audit_receipts
                         WHERE channel=? AND status='sealed' AND audit_until IS NOT NULL
                           AND audit_until<=?''', (channel, now)).fetchall()
    for row in rows:
        expired_document = {
            'format': FORMAT, 'kind': 'audit_period_expired', 'audit_id': row['id'],
            'expired_at': now,
            'statement': '超过指定审计期后，封存的标签和组规则依据已不可读取。',
        }
        db.execute('''UPDATE publication_audit_receipts
                      SET status='expired', document=?, sha256=NULL WHERE id=?''',
                   (codec.pack(expired_document), row['id']))
    return [row['id'] for row in rows]


# Descriptive aliases for callers and exploratory scripts.
audit_receipt = for_message
get_audit_receipt = for_message
get_forensics = for_message
receipt_for = original_receipt
query_audit_receipts = query
