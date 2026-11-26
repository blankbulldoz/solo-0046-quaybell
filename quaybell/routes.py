import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (GROUP, 'group_view'), (GROUP + '/pause', 'pause_view'),
        (GROUP + '/drain', 'drain_view'), (GROUP + '/drain/status', 'drain_view'),
        (GROUP + '/drains', 'drain_list'),
        (GROUP + r'/drains/([1-9][0-9]*)', 'drain_detail'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (GROUP + '/drain', 'drain'), (GROUP + '/drain/start', 'drain'),
        (GROUP + '/drain/status', 'drain'),
        (GROUP + '/pause/drain', 'drain'),
        (GROUP + '/drains', 'drain'),
        (GROUP + '/drain/retry', 'drain_retry'),
        (GROUP + '/drain/confirm', 'drain_confirm'), (GROUP + '/drain/resume', 'drain_resume'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
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
