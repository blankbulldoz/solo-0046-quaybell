import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (CHANNEL + '/supersessions', 'supersessions'), (DELIVERY, 'delivery'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP, 'group_mode'),
        (GROUP + '/pause', 'pause'),
        (GROUP + '/mode', 'group_mode'), (GROUP + '/coalesce', 'coalesce'),
        (GROUP + '/merge', 'coalesce'), (GROUP + '/conflation', 'coalesce'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), ('/v1/coalesce', 'coalesce_all'),
        (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
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
