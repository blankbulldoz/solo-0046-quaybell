from . import catalog, deadletters, evidence, forensics, inspection, overview, retention, routing
from .protocol import Reply
from .errors import missing


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
    if action == 'message_audit':
        return Reply(forensics.for_message(db, int(args[0]), now))
    if action == 'original_receipt':
        return Reply(forensics.original_receipt(db, int(args[0]), now))
    if action == 'audit_credential':
        return Reply(forensics.for_credential(db, int(args[0]), now))
    if action == 'channel_audit_credential':
        item = forensics.for_credential(db, int(args[1]), now)
        if item['channel'] != args[0]:
            raise missing('找不到该频道下的审计凭证')
        return Reply(item)
    if action == 'channel_original_receipt':
        return Reply(forensics.original_receipt(db, int(args[1]), now, args[0]))
    if action == 'audit_settings':
        return Reply(forensics.settings(db, args[0]))
    if action in ('audit_query_get', 'audit_entry'):
        raw = {key: values[-1] for key, values in (query or {}).items()}
        if action == 'audit_entry' and not any(key in raw for key in (
                'message_id', 'audit_id', *forensics.KEY_FIELDS, *forensics.TIME_FIELDS,
                *forensics.END_FIELDS, 'audit_period', 'period')):
            return Reply(forensics.settings(db, args[0]))
        return Reply(forensics.query(db, args[0], raw, now))
    raise AssertionError(action)


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))


def audit_route(db, channel, raw, now):
    return Reply(forensics.query(db, channel, raw, now))
