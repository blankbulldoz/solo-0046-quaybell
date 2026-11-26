import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/([a-z][a-z0-9-]{0,47})'
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'
PLAN = r'([1-9][0-9]{0,17})'
RETENTION_PLAN = CHANNEL + r'/retention/plans?/' + PLAN
RETENTION_PLAN_ACTION = RETENTION_PLAN + r'/(?:apply|approve|confirm|execute)'
RETENTION_COLLECTION = CHANNEL + r'/retention/plans?'
RETENTION_ACTION = CHANNEL + r'/retention/(?:apply|approve|confirm|execute)'
GLOBAL_PLAN = r'/v1/retention-plans/' + PLAN
GLOBAL_PLAN_ACTION = GLOBAL_PLAN + r'/(?:apply|approve|confirm|execute)'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (RETENTION_COLLECTION, 'retention_plans'), (RETENTION_PLAN, 'retention_plan'),
        (GLOBAL_PLAN, 'retention_plan_global'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
        (RETENTION_COLLECTION, 'retention_prepare'),
        (CHANNEL + '/retention/prepare', 'retention_prepare'),
        (CHANNEL + '/retention/preview', 'retention_prepare'),
        (RETENTION_ACTION, 'prune'),
        (RETENTION_PLAN_ACTION, 'retention_apply'), (RETENTION_PLAN, 'retention_apply'),
        (GLOBAL_PLAN_ACTION, 'retention_apply_global'), (GLOBAL_PLAN, 'retention_apply_global'),
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
