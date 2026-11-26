import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (GROUP + '/claims', 'claim'),
        (GROUP + '/wait', 'wait_claim'), (GROUP + '/wait-for-claim', 'wait_claim'),
        (GROUP + '/waiting', 'wait_claim'), (GROUP + '/poll', 'wait_claim'),
        (GROUP + '/long-poll', 'wait_claim'), (GROUP + '/long_poll', 'wait_claim'),
        (GROUP + '/longpoll', 'wait_claim'), (GROUP + '/fetch', 'wait_claim'),
        (GROUP + '/receive', 'wait_claim'), (GROUP + '/pickup', 'wait_claim'),
        (GROUP + '/claim/wait', 'wait_claim'), (GROUP + '/claims/wait', 'wait_claim'),
        (GROUP + '/claim/long-poll', 'wait_claim'),
        (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
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
