from . import codec, inspection, reconcile

# 供应商原始明细的内部字段，任何对外证据都不得包含。
INTERNAL_KEYS = frozenset(('result_digest', 'token_digest', 'token', 'receipt'))
FORBIDDEN_TOKENS = ('result_digest', 'token_digest')


def _scrub(value):
    if isinstance(value, dict):
        return {key: _scrub(item) for key, item in value.items()
                if key not in INTERNAL_KEYS and not key.startswith('_')}
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    return value


def _assert_clean(facts):
    encoded = codec.pack(facts)
    for token in FORBIDDEN_TOKENS:
        if token in encoded:
            raise RuntimeError('证据导出包含禁止外泄的内部字段')
    return encoded


def export_trace(db, message_id, now):
    facts = _scrub(inspection.trace(db, message_id))
    # 在导出快照里附上规则定义，使历史接收/拒绝在规则更换后仍可被解释。
    rules = {}
    for delivery in facts['deliveries']:
        for attempt in delivery['attempts']:
            version = attempt.get('result_rule')
            if version is not None and version not in rules:
                rule = reconcile.read(db, delivery['channel'], version)
                rules[version] = {'version': rule['version'], 'base_unit': rule['base_unit'],
                                  'tick_scale': rule['tick_scale'], 'units': rule['units']}
    data = dict(format='quaybell-trace/1', observed_at=now,
                result_rules=dict(sorted(rules.items())), trace=facts)
    canonical = codec.pack(data)
    _assert_clean(data)
    data['sha256'] = codec.fingerprint(canonical)
    return (codec.pack(data) + '\n').encode('ascii')
