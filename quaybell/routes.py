import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'
TAKEOVER_KEY = r'([A-Za-z0-9_.:-]{1,80})'
GROUP_NAME_PART = r'([a-z][a-z0-9-]{0,47})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (GROUP + '/takeover/preview', 'takeover_preview'),
        (GROUP + '/takeovers', 'takeover_list'),
        (GROUP + '/takeovers/' + TAKEOVER_KEY, 'takeover_get'),
        (GROUP + '/takeover', 'takeover_preview'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/takeover', 'channel_takeover'),
        (CHANNEL + '/takeovers', 'channel_takeover'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
        (GROUP + '/takeover/preview', 'takeover_plan'),
        (GROUP + '/takeovers/preview', 'takeover_plan'),
        (GROUP + '/takeovers/' + TAKEOVER_KEY + '/retry', 'takeover_retry'),
        (GROUP + '/takeovers/' + TAKEOVER_KEY, 'takeover_post_item'),
        (GROUP + '/takeover/' + GROUP_NAME_PART, 'takeover_to_path'),
        (GROUP + '/takeover', 'takeover'), (GROUP + '/takeovers', 'takeover'),
        (GROUP + '/take-over', 'takeover'), (GROUP + '/take-overs', 'takeover'),
        (GROUP + '/handoff', 'takeover'), (GROUP + '/handoffs', 'takeover'),
        (GROUP + '/handovers', 'takeover'), (GROUP + '/handover', 'takeover'),
        (GROUP + '/migrate', 'takeover'), (GROUP + '/migration', 'takeover'),
        (GROUP + '/succession', 'takeover'), (GROUP + '/successions', 'takeover'),
        (GROUP + '/cutover', 'takeover'), (GROUP + '/switchover', 'takeover'),
        (GROUP + '/consumer-takeover', 'takeover'),
        (GROUP + '/transfer', 'takeover_transfer'),
        (GROUP + '/moves', 'takeover_transfer'),
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
