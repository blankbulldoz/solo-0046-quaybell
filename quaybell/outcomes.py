from . import attempts, delivery, journal, policy

FAILURE_OUTCOMES = ('nacked', 'expired')


def failures(db, delivery_id):
    """实际消耗失败预算的尝试次数；主动让回（released）不计入。"""
    return db.execute(
        f"SELECT COUNT(*) FROM attempts WHERE delivery_id=? AND outcome IN ({','.join('?' * len(FAILURE_OUTCOMES))})",
        (delivery_id, *FAILURE_OUTCOMES)).fetchone()[0]


def apply(db, item, attempt, disposition, reason, now):
    if disposition == 'acked':
        state, when = 'acked', now
        extra = {}
    else:
        rule = policy.read(db, item['channel'], item['policy'])
        if disposition == 'released':
            # 主动让回不是失败：不增长失败计数、不进死信。
            # 可重领时刻固定为让回处置时间加该投递绑定策略的初始退避秒数，
            # 不按失败次数指数增长，从而既允许他人接手，又不会形成连续空转。
            used = failures(db, item['id'])
            left = max(0, rule['max_attempts'] - used)
            state, when = 'waiting', now + rule['retry_seconds']
            extra = {'worker': attempt['worker'], 'released_at': now,
                     'ready_at': when, 'reclaim_at': when,
                     'retry_after': rule['retry_seconds'],
                     'failures': used, 'max_attempts': rule['max_attempts'],
                     'remaining_failures': left, 'remaining_attempts': left}
        else:
            used = failures(db, item['id']) + 1
            if used >= rule['max_attempts']:
                state, when = 'dead', now
            else:
                origin = attempt['expires'] if disposition == 'expired' else now
                state, when = 'waiting', origin + policy.delay(rule, used)
            extra = {}
    result = dict(delivery_id=item['id'], attempt=attempt['number'], outcome=disposition,
                  state=state, effective_at=when, reason=reason) | extra
    delivery.close(db, item['id'], state, when)
    attempts.finish(db, attempt, disposition, reason, now, result)
    journal.append(db, item['channel'], now, disposition, result, item['message_id'])
    return result
