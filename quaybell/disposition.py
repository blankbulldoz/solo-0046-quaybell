from . import attempts, codec, delivery, inputs, outcomes, reconcile
from .errors import conflict, invalid


def settle(db, key, raw, now, disposition):
    optional = ('result',) if disposition == 'acked' else ()
    inputs.require(raw, ('token',) if disposition == 'acked' else ('token', 'reason'), optional)
    token = inputs.token(raw['token'], '租约凭据')
    reason = raw.get('reason')
    result_body = raw.get('result')
    if disposition == 'nacked':
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
            raise invalid('失败原因须为 1～240 个字符')
        if 'result' in raw:
            raise invalid('拒绝应答不携带处理结果')
    if disposition == 'acked' and 'result' in raw and not isinstance(result_body, dict):
        raise invalid('处理结果须为对象')
    item = delivery.get(db, key)
    attempt = attempts.by_token(db, key, token)

    # 相同请求读取已记录承诺：结果核对必须先于租约时效判断，丢响应后仍可拿回原回执。
    if attempt['outcome'] == disposition and attempt['reason'] == reason:
        return _repeated(db, item, attempt, result_body, disposition)

    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        raise _closed_lease(db, item, attempt, disposition, result_body)

    if disposition != 'acked' or result_body is None:
        receipt = outcomes.apply(db, item, attempt, disposition, reason, now)
        return receipt | {'repeated': False}

    rule = reconcile.read(db, item['channel'])
    unit, canonical, digest = reconcile.normalize(result_body, rule)
    result = {'result_rule': rule['version'], 'result_unit': unit,
              'canonical_amount': canonical, 'result_digest': digest,
              'result_reason': reconcile.ACCEPTED}
    receipt = outcomes.apply(db, item, attempt, disposition, reason, now, result)
    return receipt | {'repeated': False}


def _repeated(db, item, attempt, result_body, disposition):
    stored = codec.unpack(attempt['receipt'])
    if disposition == 'nacked':
        return stored | {'repeated': True}
    stored_has_result = attempt['result_rule'] is not None
    incoming_has_result = result_body is not None
    if stored_has_result != incoming_has_result:
        raise conflict('重交请求与原应答的结果形态不一致（带结果与不带结果不能互相替代）')
    if not stored_has_result:
        # 旧版消费者的无结果 ACK：维持原承诺，不引入新核对。
        return stored | {'repeated': True}
    if not isinstance(result_body, dict):
        raise invalid('处理结果须为对象')
    # 重交永远按接收时钉住的规则版本解释，规则升级也不改写历史承诺。
    pinned = reconcile.read(db, item['channel'], attempt['result_rule'])
    if not isinstance(result_body.get('unit'), str) or result_body['unit'] not in pinned['units']:
        raise conflict(
            f'重交单位在接收时规则 v{pinned["version"]} 中未定义，无法按原承诺核对；'
            f'已记录量为 {reconcile.describe(pinned, attempt["canonical_amount"])}')
    _, canonical, _ = reconcile.normalize(result_body, pinned)
    if reconcile.same_amount(attempt['canonical_amount'], canonical):
        return stored | {'repeated': True}
    active = reconcile.read(db, item['channel'])
    revision_note = ('' if active['version'] == pinned['version']
                     else f'；当前规则已升至 v{active["version"]}，仍按接收时 v{pinned["version"]} 判定')
    raise conflict(
        f'重交结果与已接收记录不一致：已记录 {reconcile.describe(pinned, attempt["canonical_amount"])}，'
        f'重交归一化为 {reconcile.describe(pinned, canonical)}（规则 v{pinned["version"]}）{revision_note}')


def _closed_lease(db, item, attempt, disposition, result_body):
    # 租约已结束才送达的结果：明确拒绝，并解释被拒收的归一化量，不写入任何结果事实。
    if disposition == 'acked' and result_body is not None:
        if not isinstance(result_body, dict):
            raise invalid('处理结果须为对象')
        rule = reconcile.read(db, item['channel'])
        _, canonical, _ = reconcile.normalize(result_body, rule)
        raise conflict(
            f'租约已处置或超时，结果未被接收：{reconcile.describe(rule, canonical)}'
            f'（规则 v{rule["version"]}，第 {attempt["number"]} 次尝试已结束）')
    raise conflict('租约已处置或超时')
