from . import catalog, deadletters, evidence, inspection, overview, retention, routing
from .protocol import Reply


def read(db, action, args, now, query=''):
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
        window = evidence.window_from_query(query, now)
        message_id = int(args[0])
        if window is None:
            return Reply(evidence.export_trace(db, message_id, now),
                         content_type='application/json; charset=utf-8',
                         filename=f'quaybell-trace-{args[0]}.json')
        return Reply(evidence.export_window(db, message_id, now, **window),
                     content_type='application/json; charset=utf-8',
                     filename=f"quaybell-window-{args[0]}-{window['start']}-{window['end']}.json")
    raise AssertionError(action)


def verify(raw):
    document = raw.get('document') if isinstance(raw, dict) and isinstance(raw.get('document'), dict) else raw
    expected_source = raw.get('expected_source_commitment') if isinstance(raw, dict) else None
    expected_sha = raw.get('expected_sha256') if isinstance(raw, dict) else None
    if isinstance(document, dict):
        expected_source = expected_source or document.get('expected_source_commitment')
        expected_sha = expected_sha or document.get('expected_sha256')
    return Reply(evidence.verify_document(document, expected_source, expected_sha))


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
