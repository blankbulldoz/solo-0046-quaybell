from . import catalog, comparison, deadletters, evidence, inspection, overview, retention, routing
from .protocol import Reply


def read(db, action, args, now, query=None):
    query = query or {}
    if action == 'channels':
        return Reply({'channels': catalog.list_all(db)})
    if action == 'board':
        return Reply(overview.board(db, args[0], now))
    if action == 'dead':
        return Reply({'items': deadletters.list_recent(db, args[0])})
    if action == 'trace':
        return Reply(inspection.trace(db, int(args[0])))
    if action == 'compare':
        first, second = comparison.pair_from_query(query)
        if any(key in query for key in ('evidence', 'export', 'download')):
            pair = f'{first}-vs-{second}' if first and second else 'compare'
            return Reply(evidence.export_comparison(db, int(args[0]), first, second, now),
                         content_type='application/json; charset=utf-8',
                         filename=f'quaybell-compare-{args[0]}-{pair}.json')
        doc = comparison.snapshot(db, int(args[0]), first, second, now)
        return Reply(evidence.sign(doc))
    if action == 'compare_path':
        message_id, first, second = args
        if any(key in query for key in ('evidence', 'export', 'download')):
            return Reply(evidence.export_comparison(db, int(message_id), first, second, now),
                         content_type='application/json; charset=utf-8',
                         filename=f'quaybell-compare-{message_id}-{first}-vs-{second}.json')
        doc = comparison.snapshot(db, int(message_id), first, second, now)
        return Reply(evidence.sign(doc))
    if action == 'retention':
        return Reply(retention.plan(db, args[0], now))
    if action == 'export':
        first, second = comparison.pair_from_query(query)
        if first and second:
            pair = f'{first}-vs-{second}'
            return Reply(evidence.export_comparison(db, int(args[0]), first, second, now),
                         content_type='application/json; charset=utf-8',
                         filename=f'quaybell-compare-{args[0]}-{pair}.json')
        return Reply(evidence.export_trace(db, int(args[0]), now),
                     content_type='application/json; charset=utf-8',
                     filename=f'quaybell-trace-{args[0]}.json')
    if action == 'export_compare':
        first, second = comparison.pair_from_query(query)
        pair = f'{first}-vs-{second}' if first and second else 'compare'
        return Reply(evidence.export_comparison(db, int(args[0]), first, second, now),
                     content_type='application/json; charset=utf-8',
                     filename=f'quaybell-compare-{args[0]}-{pair}.json')
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))


def compare_from_body(db, message_id, raw, now):
    from .errors import invalid
    if isinstance(message_id, str) and message_id.isdigit():
        message_id = int(message_id)
    if not isinstance(message_id, int) or isinstance(message_id, bool):
        raise invalid('对照请求须在路径或 message_id 中给出消息标识')
    if message_id <= 0:
        raise invalid('消息标识无效')
    first, second = comparison.pair_from_body(raw)
    doc = comparison.snapshot(db, message_id, first, second, now)
    return Reply(evidence.sign(doc))


def verify(raw):
    document = raw.get('document') if isinstance(raw, dict) and isinstance(raw.get('document'), dict) else raw
    return Reply(evidence.verify_document(document))
