import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'
AUDIT_DOCUMENT = r'/v1/(?:audit-credentials|audit-receipts|publication-audits|forensic-receipts|forensics|receipts|audits)/([1-9][0-9]{0,17})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (MESSAGE + '/audit', 'message_audit'), (MESSAGE + '/forensics', 'message_audit'),
        (MESSAGE + '/receipt', 'original_receipt'), (MESSAGE + '/receipt/audit', 'message_audit'),
        (MESSAGE + '/publication-audit', 'message_audit'),
        (r'/v1/publications/([1-9][0-9]{0,17})/audit', 'message_audit'),
        (AUDIT_DOCUMENT, 'audit_credential'),
        (CHANNEL + r'/audit-credentials/([1-9][0-9]{0,17})', 'channel_audit_credential'),
        (CHANNEL + r'/audit-receipts/([1-9][0-9]{0,17})', 'channel_audit_credential'),
        (CHANNEL + r'/receipts/([1-9][0-9]{0,17})', 'channel_original_receipt'),
        (CHANNEL + r'/audits/([1-9][0-9]{0,17})', 'channel_audit_credential'),
        (CHANNEL + r'/forensics/([1-9][0-9]{0,17})', 'channel_audit_credential'),
        (CHANNEL + '/audit', 'audit_entry'), (CHANNEL + '/audits', 'audit_query_get'),
        (CHANNEL + '/audit-credentials', 'audit_query_get'),
        (CHANNEL + '/audit-receipt', 'audit_query_get'),
        (CHANNEL + '/audit-receipts', 'audit_query_get'),
        (CHANNEL + '/receipts/audit', 'audit_query_get'),
        (CHANNEL + '/receipts', 'audit_query_get'),
        (CHANNEL + '/publication-audits', 'audit_query_get'),
        (CHANNEL + '/forensics', 'audit_query_get'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
        (CHANNEL + '/audit/config', 'audit_config'), (CHANNEL + '/audit/query', 'audit_query'),
        (CHANNEL + '/audit', 'audit_write_entry'),
        (CHANNEL + '/forensics', 'audit_query'), (CHANNEL + '/audits', 'audit_query'),
        (CHANNEL + '/audit-credentials', 'audit_query'),
        (CHANNEL + '/audit-receipt', 'audit_query'),
        (CHANNEL + '/audits/query', 'audit_query'),
        (CHANNEL + '/audit-receipts', 'audit_query'),
        (CHANNEL + '/receipts/audit', 'audit_query'),
        (CHANNEL + '/receipts', 'audit_query'),
        (CHANNEL + '/publication-audits', 'audit_query'),
        (AUDIT_DOCUMENT + '/verify', 'audit_verify'),
    ),
}
ROUTES = {method: [(re.compile(pattern), action) for pattern, action in patterns]
          for method, patterns in PATTERNS.items()}


def match(method, path):
    for pattern, action in ROUTES.get(method, []):
        result = pattern.fullmatch(path)
        if result:
            return action, result.groups()
    return None
