from . import catalog, deadletters, evidence, inspection, overview, retention, reviews, routing
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
    if action == 'candidate_list':
        return Reply(reviews.list_candidates(db, args[0]))
    if action == 'candidate_get':
        return Reply(reviews.get_candidate(db, int(args[1]), channel=args[0]))
    if action == 'candidate_get_global':
        return Reply(reviews.get_candidate(db, int(args[0])))
    if action == 'export':
        return Reply(evidence.export_trace(db, int(args[0]), now),
                     content_type='application/json; charset=utf-8',
                     filename=f'quaybell-trace-{args[0]}.json')
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
