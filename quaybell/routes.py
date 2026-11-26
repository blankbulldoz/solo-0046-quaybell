import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
GROUPNAME = r'([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'),
        (MESSAGE + '/evidence', 'export'),
        (MESSAGE + '/compare', 'compare'),
        (MESSAGE + '/comparison', 'compare'),
        (MESSAGE + '/contrast', 'compare'),
        (MESSAGE + '/investigation', 'compare'),
        (MESSAGE + '/investigate', 'compare'),
        (MESSAGE + '/compare/' + GROUPNAME + '/' + GROUPNAME, 'compare_path'),
        (MESSAGE + '/comparison/' + GROUPNAME + '/' + GROUPNAME, 'compare_path'),
        (MESSAGE + '/contrast/' + GROUPNAME + '/' + GROUPNAME, 'compare_path'),
        (MESSAGE + '/compare/evidence', 'export_compare'),
        (MESSAGE + '/comparison/evidence', 'export_compare'),
        (MESSAGE + '/compare/export', 'export_compare'),
        (MESSAGE + '/comparison/export', 'export_compare'),
        (CHANNEL + '/retention', 'retention'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
        ('/v1/messages/compare', 'compare_body'),
        (MESSAGE + '/compare', 'compare_post'),
        (MESSAGE + '/comparison', 'compare_post'),
        ('/v1/compare', 'compare_body'),
        ('/v1/comparison', 'compare_body'),
        ('/v1/evidence/verify', 'verify_evidence'),
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
