import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'
CANDIDATE_KINDS = (r'(?:candidates|publication-candidates|publication-reviews|reviews|'
                   r'conflicts|resolutions|drafts|intents)')
CANDIDATE_SCOPED = CHANNEL + r'/' + CANDIDATE_KINDS
CANDIDATE_GLOBAL = r'/v1/' + CANDIDATE_KINDS

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (CANDIDATE_SCOPED, 'candidate_list'),
        (CANDIDATE_SCOPED + r'/([1-9][0-9]{0,17})', 'candidate_get'),
        (CANDIDATE_GLOBAL + r'/([1-9][0-9]{0,17})', 'candidate_get_global'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (MESSAGE + r'/(?:resend|republish)', 'resend_message'),
        (CANDIDATE_SCOPED, 'candidate_create'),
        (CANDIDATE_SCOPED + r'/([1-9][0-9]{0,17})/confirm', 'candidate_confirm'),
        (CANDIDATE_SCOPED + r'/([1-9][0-9]{0,17})/(?:discard|cancel)', 'candidate_discard'),
        (CANDIDATE_GLOBAL + r'/([1-9][0-9]{0,17})/confirm', 'candidate_confirm_global'),
        (CANDIDATE_GLOBAL + r'/([1-9][0-9]{0,17})/(?:discard|cancel)', 'candidate_discard_global'),
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
