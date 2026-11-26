from . import codec, comparison, inspection

COMPARE_FORMAT = 'quaybell-compare/1'


def sign(document):
    """按既有证据约定，对除 sha256 外的规范化内容计算摘要。"""
    body = {key: value for key, value in document.items() if key != 'sha256'}
    return body | {'sha256': codec.fingerprint(codec.pack(body))}


def export_trace(db, message_id, now):
    facts = inspection.trace(db, message_id)
    data = dict(format='quaybell-trace/1', observed_at=now, trace=facts)
    data['sha256'] = codec.fingerprint(codec.pack(data))
    return (codec.pack(data) + '\n').encode('ascii')


def export_comparison(db, message_id, first, second, now):
    snapshot = comparison.snapshot(db, message_id, first, second, now)
    document = sign(snapshot)
    return (codec.pack(document) + '\n').encode('ascii')


def verify_document(document):
    """复核导出/共享快照的摘要是否仍与内容一致，并回报它绑定的观察时刻。"""
    if not isinstance(document, dict) or 'sha256' not in document:
        return {'valid': False, 'reason': '缺少 sha256 摘要，无法核对',
                'format': document.get('format') if isinstance(document, dict) else None}
    claimed = document['sha256']
    body = {key: value for key, value in document.items() if key != 'sha256'}
    actual = codec.fingerprint(codec.pack(body))
    valid = isinstance(claimed, str) and _constant_time_equal(claimed, actual)
    return {'valid': valid,
            'reason': None if valid else '摘要不符：快照内容已被改动或复制不完整',
            'format': document.get('format'),
            'observed_at': document.get('observed_at'),
            'message_id': document.get('message_id'),
            'groups': document.get('groups'),
            'sha256': claimed,
            'recomputed_sha256': actual}


def _constant_time_equal(left, right):
    if len(left) != len(right):
        return False
    result = 0
    for x, y in zip(left.encode('ascii', 'replace'), right.encode('ascii', 'replace')):
        result |= x ^ y
    return result == 0
