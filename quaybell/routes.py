import re

CHANNEL = r'/v1/channels/([a-z][a-z0-9-]{0,47})'
NAME = r'([a-z][a-z0-9-]{0,47})'
GROUP = CHANNEL + r'/groups/' + NAME
GROUP_ASSIGNMENT_BY_NAME = CHANNEL + r'/(?:assignments|assignment|partitions|partition|shards)/' + NAME
MESSAGE = r'/v1/messages/([1-9][0-9]{0,17})'
DELIVERY = r'/v1/deliveries/([1-9][0-9]{0,17})'
WORKER = r'([A-Za-z0-9][A-Za-z0-9_.:-]{0,63})'

PATTERNS = {
    'GET': (
        ('/v1/channels', 'channels'), (CHANNEL, 'board'), (CHANNEL + '/dead', 'dead'),
        (CHANNEL + '/groups', 'groups_list'), (GROUP, 'group_detail'),
        (MESSAGE, 'trace'), (MESSAGE + '/evidence', 'export'), (CHANNEL + '/retention', 'retention'),
        (CHANNEL + r'/partitions', 'channel_assignment'),
        (CHANNEL + r'/assignment', 'channel_assignment'),
        (CHANNEL + r'/assignments', 'channel_assignment'),
        (GROUP_ASSIGNMENT_BY_NAME, 'group_assignment'),
        (GROUP + r'/partitions', 'group_assignment'),
        (GROUP + r'/partition', 'group_assignment'),
        (GROUP + r'/partition-assignments', 'group_assignment'),
        (GROUP + r'/shard-assignments', 'group_assignment'),
        (GROUP + r'/assignment', 'group_assignment'),
        (GROUP + r'/assignments', 'group_assignment'),
        (GROUP + r'/topology', 'group_assignment'),
        (GROUP + r'/ownership', 'ownership'),
        (GROUP + r'/keys', 'ownership'),
        (GROUP + r'/members', 'members'),
        (GROUP + r'/workers', 'members'),
        (GROUP + r'/partition-assignment', 'group_assignment'),
    ),
    'POST': (
        ('/v1/channels', 'create_channel'), (CHANNEL + '/policy', 'revision'),
        (CHANNEL + '/groups', 'subscribe'), (GROUP + '/pause', 'pause'),
        (CHANNEL + '/preview', 'preview'), (CHANNEL + '/messages', 'publish'),
        (GROUP + '/claim', 'claim'), (DELIVERY + '/ack', 'ack'), (DELIVERY + '/nack', 'nack'),
        ('/v1/sweep', 'sweep'), (DELIVERY + '/redrive', 'redrive'), (CHANNEL + '/retention', 'prune'),
        (GROUP + r'/partitions', 'configure_assignment'),
        (GROUP + r'/partition', 'configure_assignment'),
        (GROUP + r'/partition-assignments', 'configure_assignment'),
        (GROUP + r'/partition-assignment', 'configure_assignment'),
        (GROUP + r'/shard-assignments', 'configure_assignment'),
        (GROUP + r'/assignment', 'configure_assignment'),
        (GROUP + r'/assignments', 'configure_assignment'),
        (GROUP + r'/topology', 'configure_assignment'),
        (GROUP + r'/assign', 'configure_assignment'),
        (GROUP + r'/reassign', 'rebalance'),
        (GROUP + r'/rebalance', 'rebalance'),
        (GROUP + r'/join', 'join'),
        (GROUP + r'/fence', 'fence_leases'),
        (GROUP + r'/takeover', 'fence_leases'),
        (GROUP + r'/revoke', 'fence_leases'),
        (GROUP + r'/authorize', 'authorize'),
        (GROUP + r'/authorization', 'authorize'),
        (GROUP + r'/entitlements', 'authorize'),
        (GROUP + r'/owns', 'authorize'),
        (GROUP + r'/members', 'member_touch'),
        (GROUP + r'/workers', 'member_touch'),
        (GROUP + r'/heartbeat', 'member_touch'),
        (GROUP + r'/members/heartbeat', 'member_touch'),
        (GROUP + r'/leave', 'member_leave'),
        (GROUP + r'/members/leave', 'member_leave'),
        (GROUP + r'/members/' + WORKER + r'/heartbeat', 'member_heartbeat_path'),
        (GROUP + r'/workers/' + WORKER + r'/heartbeat', 'member_heartbeat_path'),
        (GROUP + r'/members/' + WORKER + r'/leave', 'member_leave_path'),
        (GROUP + r'/workers/' + WORKER + r'/leave', 'member_leave_path'),
        (CHANNEL + r'/partitions', 'channel_configure'),
        (CHANNEL + r'/partition', 'channel_configure'),
        (CHANNEL + r'/assignment', 'channel_configure'),
        (CHANNEL + r'/assignments', 'channel_configure'),
        (CHANNEL + r'/rebalance', 'channel_configure'),
        (GROUP_ASSIGNMENT_BY_NAME, 'configure_assignment'),
    ),
}
ROUTES = {method: [(re.compile(pattern), action) for pattern, action in patterns]
          for method, patterns in PATTERNS.items()}


def match(method, path):
    candidate = path[:-1] if len(path) > 1 and path.endswith('/') else path
    for pattern, action in ROUTES.get(method, []):
        result = pattern.fullmatch(candidate)
        if result:
            return action, result.groups()
    return None
