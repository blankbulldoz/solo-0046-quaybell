from . import catalog, confluence_service, deadletters, evidence, inspection, overview, retention, routing, subscriptions
from .protocol import Reply


def read(db, action, args, now):
    if action == 'channels':
        return Reply({'channels': catalog.list_all(db)})
    if action == 'group_detail':
        group = subscriptions.get(db, args[0], args[1])
        if confluence_service.is_enabled(db, group['id']):
            merge = confluence_service.snapshot(db, args[0], args[1], now)
            return Reply(group | {'merge_mode': 'confluence', 'confluence': merge})
        return Reply(group | {'merge_mode': 'all', 'confluence': {'enabled': False, 'mode': 'all'}})
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
    if action == 'group_mode_read':
        group = subscriptions.get(db, args[0], args[1])
        if confluence_service.is_enabled(db, group['id']):
            return Reply(confluence_service.snapshot(db, args[0], args[1], now))
        return Reply({'channel': args[0], 'group': args[1], 'enabled': False, 'mode': 'all'})
    if action == 'confluence_snapshot':
        return Reply(confluence_service.snapshot(db, args[0], args[1], now))
    if action == 'confluence_gaps':
        return Reply(confluence_service.gaps(db, args[0], args[1], now))
    if action == 'confluence_late':
        data = confluence_service.snapshot(db, args[0], args[1], now)
        return Reply({'channel': args[0], 'group': args[1],
                      'items': data['late_items'], 'late_arrivals': data['late_arrivals'],
                      'traceable_items': data['traceable_items']})
    if action == 'confluence_incidents':
        data = confluence_service.snapshot(db, args[0], args[1], now)
        return Reply({'channel': args[0], 'group': args[1], 'items': data['incidents'],
                      'incidents': data['incidents']})
    if action == 'confluence_group_record':
        return Reply(confluence_service.group_trace(db, args[0], args[1], int(args[2])))
    if action == 'confluence_trace':
        return Reply(confluence_service.global_trace(db, int(args[0])))
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
