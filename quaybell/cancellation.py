from . import delivery, inputs, journal
from .errors import conflict, invalid
from .codec import unpack


def cancel_first_wait(db, key, raw, now):
    inputs.require(raw, (), ('reason',))
    reason = raw.get('reason')
    if reason is not None and (not isinstance(reason, str) or not reason.strip() or len(reason) > 240):
        raise invalid('取消原因须为 1～240 个字符')
    item = delivery.get(db, key)
    if item['state'] == 'cancelled':
        record = next((row for row in db.execute(
            'SELECT details FROM journal WHERE message_id=? AND kind=? ORDER BY id DESC',
            (item['message_id'], 'cancelled'))
            if unpack(row['details']).get('delivery_id') == key), None)
        details = unpack(record['details']) if record is not None else {}
        return dict(delivery_id=key, state='cancelled', effective_at=item['terminal_at'],
                    reason=details.get('reason'), repeated=True)
    if delivery.cancel(db, key, now) != 1:
        raise conflict('该投递已被领取或已处置，无法取消')
    result = dict(delivery_id=key, state='cancelled', effective_at=now, reason=reason, repeated=False)
    journal.append(db, item['channel'], now, 'cancelled', result, item['message_id'])
    return result
