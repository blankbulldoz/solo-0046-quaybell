import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'
CONFLUENCE_RECORD = r'/v1/confluence-records/([1-9][0-9]{0,17})'
LOCAL_RECORD = r'/([1-9][0-9]{0,17})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (GROUP, 'group_detail'),
        (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (GROUP + '/confluence', 'confluence_snapshot'),
        (GROUP + '/mode', 'group_mode_read'),
        (GROUP + '/confluence/state', 'confluence_snapshot'),
        (GROUP + '/confluence/status', 'confluence_snapshot'),
        (GROUP + '/confluence/gaps', 'confluence_gaps'),
        (GROUP + '/confluence/late', 'confluence_late'),
        (GROUP + '/confluence/incidents', 'confluence_incidents'),
        (GROUP + '/merge', 'confluence_snapshot'),
        (GROUP + '/merge/state', 'confluence_snapshot'),
        (GROUP + '/merge/status', 'confluence_snapshot'),
        (GROUP + '/merge/gaps', 'confluence_gaps'),
        (GROUP + '/merge/late', 'confluence_late'),
        (GROUP + '/merge/incidents', 'confluence_incidents'),
        (GROUP + '/gaps', 'confluence_gaps'),
        (GROUP + '/sequences', 'confluence_snapshot'),
        (GROUP + '/late-arrivals', 'confluence_late'),
        (GROUP + '/confluence/records' + LOCAL_RECORD, 'confluence_group_record'),
        (GROUP + '/merge/records' + LOCAL_RECORD, 'confluence_group_record'),
        (CONFLUENCE_RECORD, 'confluence_trace'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP, 'group_update'),
        (GROUP + '/mode', 'group_mode'),
        (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
        (GROUP + '/confluence/records/' + r'([1-9][0-9]{0,17})/reconcile', 'confluence_reconcile'),
        (GROUP + '/merge/records/' + r'([1-9][0-9]{0,17})/reconcile', 'confluence_reconcile'),
        (GROUP + '/confluence/records', 'confluence_submit'),
        (GROUP + '/confluence/events', 'confluence_submit'),
        (GROUP + '/confluence/ingest', 'confluence_submit'),
        (GROUP + '/confluence/submit', 'confluence_submit'),
        (GROUP + '/merge/records', 'confluence_submit'),
        (GROUP + '/merge/events', 'confluence_submit'),
        (GROUP + '/merge/ingest', 'confluence_submit'),
        (GROUP + '/merge/submit', 'confluence_submit'),
        (GROUP + '/sequences', 'confluence_submit'),
        (GROUP + '/sequence', 'confluence_submit'),
        (GROUP + '/advance', 'confluence_advance'),
        (GROUP + '/tick', 'confluence_advance'),
        (GROUP + '/confluence/advance', 'confluence_advance'),
        (GROUP + '/confluence/tick', 'confluence_advance'),
        (GROUP + '/confluence/advance-time', 'confluence_advance'),
        (GROUP + '/merge/advance', 'confluence_advance'),
        (GROUP + '/merge/tick', 'confluence_advance'),
        (GROUP + '/merge/advance-time', 'confluence_advance'),
        (GROUP + '/confluence', 'confluence_configure'),
        (GROUP + '/confluence-mode', 'confluence_configure'),
        (GROUP + '/merge-mode', 'confluence_configure'),
        (GROUP + '/merge', 'confluence_upsert'),
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
