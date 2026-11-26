from . import attempts, codec, delivery, inputs, outcomes
from .errors import conflict, invalid


def takeover_fence(db, delivery_id):
    return db.execute('''SELECT ti.takeover_id, ti.target_delivery_id, ti.source_delivery_id,
                                ti.lease_attempt, g.client_key AS takeover_key,
                                ss.name AS source_group, ts.name AS target_group
                         FROM takeover_items ti
                         JOIN group_takeovers g ON g.id=ti.takeover_id
                         JOIN subscriptions ss ON ss.id=g.source_group_id
                         JOIN subscriptions ts ON ts.id=g.target_group_id
                         WHERE ti.source_delivery_id=?''', (delivery_id,)).fetchone()


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

    fence = takeover_fence(db, key)
    if fence is not None and (item['state'] == 'transferred' or attempt['outcome'] == 'superseded'):
        details = dict(
            reason='lease_superseded', code='lease_superseded', lease_status='superseded',
            late_confirmation='rejected', outcome='superseded', conflict_family='conflict',
            source_delivery_id=key, old_delivery_id=key,
            source_group=fence['source_group'], old_group=fence['source_group'],
            target_group=fence['target_group'], new_group=fence['target_group'],
            target_delivery_id=fence['target_delivery_id'],
            new_delivery_id=fence['target_delivery_id'],
            takeover_id=fence['takeover_id'], takeover_key=fence['takeover_key'],
        )
        raise conflict('旧租约已在消费组接管时终止，晚到的应答不生效；未完成项已归属新组', details)

    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        raise conflict('租约已处置或超时')
    return outcomes.apply(db, item, attempt, disposition, reason, now) | {'repeated': False}
