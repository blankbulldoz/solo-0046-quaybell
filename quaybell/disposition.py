from . import attempts, codec, delivery, inputs, outcomes
from .errors import conflict, invalid


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
        return codec.unpack(attempt['receipt']) | {'repeated': True}
    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        if attempt['reason'] and ('围栏' in attempt['reason']):
            raise conflict('该租约属于已失效的旧分片属主版本，须由当前有权进程重新取件处置')
        raise conflict('租约已处置或超时')
    return outcomes.apply(db, item, attempt, disposition, reason, now) | {'repeated': False}
