from . import attempts, codec, delivery, inputs, outcomes
from .errors import conflict, invalid


def settle(db, key, raw, now, disposition):
    if disposition == 'nacked':
        inputs.require(raw, ('token', 'reason'))
    elif disposition == 'released':
        inputs.require(raw, ('token',), ('reason', 'worker'))
        if raw.get('worker') is not None:
            inputs.name(raw['worker'], '工作进程')
    else:
        inputs.require(raw, ('token',))
    token = inputs.token(raw['token'], '租约凭据')
    reason = raw.get('reason')
    if disposition == 'nacked' and (not isinstance(reason, str) or not reason.strip()
                                   or len(reason) > 240):
        raise invalid('失败原因须为 1～240 个字符')
    if disposition == 'released' and reason is not None and (
            not isinstance(reason, str) or not reason.strip() or len(reason) > 240):
        raise invalid('让回备注须为 1～240 个字符')
    item = delivery.get(db, key)
    attempt = attempts.by_token(db, key, token)
    if attempt['outcome'] == disposition and attempt['reason'] == reason:
        return codec.unpack(attempt['receipt']) | {'repeated': True}
    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        raise conflict('租约已处置或超时')
    return outcomes.apply(db, item, attempt, disposition, reason, now) | {'repeated': False}
