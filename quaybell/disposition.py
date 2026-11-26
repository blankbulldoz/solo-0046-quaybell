from . import attempts, codec, delivery, failures, inputs, outcomes
from .errors import conflict, invalid


def settle(db, key, raw, now, disposition, forced_classification=None):
    if disposition == 'acked':
        inputs.require(raw, ('token',))
        token = inputs.token(raw['token'], '租约凭据')
        reason = classification = retry_after_seconds = None
    else:
        failures.require_nack_fields(raw)
        token = inputs.token(raw['token'], '租约凭据')
        reason = failures.reason_text(raw)
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
            raise invalid('失败原因须为 1～240 个字符')
        classification, retry_after_seconds = failures.extract(raw)
        if forced_classification is not None:
            classification = forced_classification
    item = delivery.get(db, key)
    attempt = attempts.by_token(db, key, token)
    # 相同应答（同凭据、同处置、同原因）的重试直接读取已记录回执；
    # 分类只影响首次落库的处置走向，回执本身已含完整分类结果。
    if attempt['outcome'] == disposition and attempt['reason'] == reason:
        return codec.unpack(attempt['receipt']) | {'repeated': True}
    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        raise conflict('租约已处置或超时')
    return outcomes.apply(db, item, attempt, disposition, reason, now,
                          classification=classification,
                          retry_after_seconds=retry_after_seconds) | {'repeated': False}
