from . import catalog, deadletters, evidence, evidence_stream, inspection, overview, retention, routing
from .protocol import Reply, StreamReply

STREAM_TYPES = ('application/x-ndjson', 'application/jsonl')


def _wants_stream(handler):
    query = handler.path.split('?', 1)[1] if '?' in handler.path else ''
    for pair in query.split('&'):
        if not pair:
            continue
        key, _, value = pair.partition('=')
        if key == 'format' and value in ('stream', '2'):
            return True
        if key == 'format' and value in ('object', '1'):
            return False
    accept = handler.headers.get('Accept', '')
    offered = [part.split(';', 1)[0].strip().lower() for part in accept.split(',') if part.strip()]
    if any(kind in offered for kind in STREAM_TYPES):
        if 'application/json' in offered and offered.index('application/json') < min(
                offered.index(kind) for kind in STREAM_TYPES if kind in offered):
            return False
        return True
    return False


def read(db, action, args, now, request=None):
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
        if request is not None and _wants_stream(request):
            return _stream_reply(db, args, now)
        return Reply(evidence.export_trace(db, int(args[0]), now),
                     content_type='application/json; charset=utf-8',
                     filename=f'quaybell-trace-{args[0]}.json')
    if action == 'export_stream':
        return _stream_reply(db, args, now)
    raise AssertionError(action)


def _stream_reply(db, args, now):
    message_id = int(args[0])
    frames = evidence_stream.stream_trace(db, message_id, now)
    return StreamReply(frames,
                       content_type=evidence_stream.STREAM_MEDIA,
                       filename=f'quaybell-trace-{args[0]}.ndjson')


def check_route(db, channel, raw):
    return Reply(routing.preview(db, channel, raw))
