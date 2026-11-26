from . import catalog, deadletters, evidence, inspection, overview, retention, routing
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
    if action == 'retention_plans':
        return Reply(retention.list_plans(db, args[0]))
    if action == 'retention_plan':
        return Reply(retention.fetch(db, args[0], int(args[1])))
    if action == 'retention_plan_global':
        return Reply(retention.fetch_any(db, int(args[0])))
    if action == 'export':
        return Reply(evidence.export_trace(db, int(args[0]), now),
                     content_type='application/json; charset=utf-8',
                     filename=f'quaybell-trace-{args[0]}.json')
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
