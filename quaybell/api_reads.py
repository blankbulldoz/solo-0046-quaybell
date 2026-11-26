from . import catalog, deadletters, evidence, inspection, overview, retention, routing, takeover
from .protocol import Reply


def read(db, action, args, now):
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
    if action == 'takeover_preview':
        return Reply(takeover.preview(db, args[0], args[1], now))
    if action == 'takeover_list':
        return Reply(takeover.list_takeovers(db, args[0], args[1]))
    if action == 'takeover_get':
        return Reply(takeover.get_takeover(db, args[0], args[1], args[2], now))
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
