from . import attempts, codec, delivery, expiry, inputs, outcomes
from .errors import conflict, invalid

DEADLINE_NOTICE = '消息已超过处理期限，投递已转入死信，该应答不计为成功'


def settle(db, key, raw, now, disposition):
    inputs.require(raw, ('token',) if disposition == 'acked' else ('token', 'reason'))
    token = inputs.token(raw['token'], '租约凭据')
    reason = raw.get('reason')
    if disposition == 'nacked' and (not isinstance(reason, str) or not reason.strip()
                                   or len(reason) > 240):
        raise invalid('失败原因须为 1～240 个字符')
    item = delivery.get(db, key)
    attempt = attempts.by_token(db, key, token)
    if attempt['outcome'] == disposition and attempt['reason'] == reason:
        return codec.unpack(attempt['receipt']) | {'repeated': True}, 200
    if attempt['outcome'] == 'expired' and attempt['reason'] == outcomes.DEADLINE:
        return codec.unpack(attempt['receipt']) | {'repeated': True, 'accepted': False}, 409
    if item['terminal_reason'] == outcomes.DEADLINE or (
            item['state'] in ('waiting', 'leased') and delivery.stale(item, now)):
        # The deadline closed this attempt as expired for the deadline; an ack or
        # nack arriving afterwards is reported but never counted as a success.
        result = expiry.terminate(db, item, now) if item['state'] != 'dead' else None
        base = result or dict(delivery_id=item['id'], attempt=attempt['number'], outcome='expired',
                              state='dead', reason=outcomes.DEADLINE,
                              terminal_reason=outcomes.DEADLINE)
        return dict(base) | {'accepted': False, 'repeated': False, 'message': DEADLINE_NOTICE}, 409
    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        raise conflict('租约已处置或超时')
    return outcomes.apply(db, item, attempt, disposition, reason, now) | {'repeated': False}, 200
