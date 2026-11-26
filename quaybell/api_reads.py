from . import (assignments, catalog, deadletters, evidence, inspection, overview,
               retention, routing, subscriptions)
from .protocol import Reply


def read(db, action, args, now, query=None):
    if action == 'channels':
        return Reply({'channels': catalog.list_all(db)})
    if action == 'board':
        return Reply(overview.board(db, args[0], now))
    if action == 'dead':
        return Reply({'items': deadletters.list_recent(db, args[0])})
    if action == 'trace':
        return Reply(inspection.trace(db, int(args[0])))
    if action == 'retention':
        return Reply(retention.plan(db, args[0], now))
    if action == 'export':
        return Reply(evidence.export_trace(db, int(args[0]), now),
                     content_type='application/json; charset=utf-8',
                     filename=f'quaybell-trace-{args[0]}.json')
    if action in ('group_assignment', 'ownership'):
        return Reply(assignments.view(db, args[0], args[1], now, query))
    if action == 'members':
        return Reply(assignments.member_view(db, args[0], args[1]))
    if action == 'channel_assignment':
        return Reply(assignments.channel_view(db, args[0], now, query))
    if action == 'group_detail':
        group = subscriptions.get(db, args[0], args[1])
        return Reply(group | {'assignment': assignments.view(db, args[0], args[1], now, query)})
    if action == 'groups_list':
        groups = [group | {'assignment': assignments.summary(db, args[0], group['name'])}
                  for group in subscriptions.for_channel(db, args[0])]
        return Reply({'channel': args[0], 'groups': groups})
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
