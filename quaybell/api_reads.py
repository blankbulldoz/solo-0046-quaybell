from . import batches, catalog, deadletters, evidence, inspection, overview, retention, routing
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
    if action == 'batch_list':
        return Reply(batches.list_batches(db, args[0]))
    if action == 'batch_parts':
        return Reply(batches.parts_view(db, args[0], args[1]))
    if action == 'batch_part_get':
        return Reply(batches.part_get(db, args[0], args[1], args[2]))
    if action == 'batch_get':
        return Reply(batches.describe(db, batches.resolve(db, args[0], args[1])))
    if action == 'batch_receipt':
        return Reply(batches.get_receipt(db, args[0], args[1]))
    if action == 'batch_parts_global':
        return Reply(batches.parts_view(db, None, args[0]))
    if action == 'batch_part_get_global':
        return Reply(batches.part_get(db, None, args[0], args[1]))
    if action == 'batch_get_global':
        return Reply(batches.describe(db, batches.resolve(db, None, args[0])))
    if action == 'batch_receipt_global':
        return Reply(batches.get_receipt(db, None, args[0]))
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
