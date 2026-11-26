from urllib.parse import parse_qs

from . import catalog, inputs
from .codec import unpack
from .errors import invalid

MAX_LIMIT = 50
DEFAULT_LIMIT = 30


def watermark(db, channel, removed):
    row = db.execute('SELECT MAX(id) AS top FROM messages WHERE channel=?', (channel,)).fetchone()
    return max([row['top'] or 0, *removed])


def pruned_ids(db, channel):
    """Ids removed by this channel's retention runs, from the channel-level journal."""
    ids = set()
    for row in db.execute("SELECT details FROM journal WHERE channel=? AND kind='pruned' ORDER BY id",
                          (channel,)):
        ids.update(unpack(row['details']).get('message_ids', ()))
    return ids


def pruned_source(db, message_id):
    """The retention record that removed a message, searched across channels."""
    for row in db.execute("SELECT channel,at,details FROM journal WHERE kind='pruned' ORDER BY id DESC"):
        details = unpack(row['details'])
        if message_id in details.get('message_ids', ()):
            return dict(row) | {'details': details}
    return None


def page(db, channel, now, *, bound=None, before=None, limit=DEFAULT_LIMIT):
    """One bounded slice of the channel's message review list.

    ``bound`` is the inclusive upper id edge and is the identity of the reviewed
    range. A client must keep it unchanged while paging so that messages
    published after the first screen (higher ids) never shift or skip rows.
    ``before`` is this request's exclusive upper edge, normally the previous
    response's ``next_cursor``. Messages retain their ids for their lifetime and
    retention deletes whole rows, so the ids still present in ``(floor, before)``
    are exactly the uninterrupted remainder of the range; ids the channel's
    retention removed inside the inspected span are reported in ``pruned``
    rather than silently skipped. ``observed_at`` is the instant this slice was
    read; it does not freeze later detail reads.
    """
    catalog.get(db, channel)
    removed = pruned_ids(db, channel)
    latest = watermark(db, channel, removed)
    if bound is None:
        bound = latest
    cursor = bound + 1 if before is None else before
    rows = [dict(row) for row in db.execute('''
        SELECT id,producer,dedup_key,lane,policy,published,available
        FROM messages WHERE channel=? AND id<=? AND id<? ORDER BY id DESC LIMIT ?''',
        (channel, bound, cursor, limit + 1))]
    more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = rows[-1]['id'] if more else None
    floor = rows[-1]['id'] if next_cursor is not None else 0
    pruned = sorted((key for key in removed
                     if floor < key < cursor and key <= bound), reverse=True)
    return {
        'page': {'channel': channel, 'bound': bound, 'latest': latest,
                 'cursor': cursor, 'next_cursor': next_cursor, 'limit': limit,
                 'complete': next_cursor is None, 'observed_at': now},
        'messages': rows,
        'pruned': pruned,
    }


def options(query):
    """Parse and validate the review query string without trusting its values."""
    params = parse_qs(query, keep_blank_values=True)
    if set(params) - {'bound', 'before', 'limit'}:
        raise invalid('消息审阅只接受 bound、before 和 limit')
    values = {}
    for key in params:
        if len(params[key]) != 1:
            raise invalid(f'{key} 只能出现一次')
        raw = params[key][0]
        if not raw.isascii() or not raw.isdecimal():
            raise invalid(f'{key} 须为十进制整数')
        values[key] = int(raw)
    bound = None if 'bound' not in values else inputs.integer(values['bound'], 0, 2**63-1, '范围上限')
    before = None if 'before' not in values else inputs.integer(values['before'], 1, 2**63-1, '翻页游标')
    limit = inputs.integer(values.get('limit', DEFAULT_LIMIT), 1, MAX_LIMIT, '每页条数')
    if before is not None and bound is None:
        raise invalid('翻页须同时携带范围上限 bound')
    if before is not None and before > bound + 1:
        raise invalid('翻页游标须落在范围上限之内')
    return {'bound': bound, 'before': before, 'limit': limit}
