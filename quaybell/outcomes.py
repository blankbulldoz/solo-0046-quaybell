from . import attempts, delivery, journal, policy

LEASE_TIMEOUT = '租约超时'
DEADLINE = '超过处理期限'


def apply(db, item, attempt, disposition, reason, now, terminal=False, kind=None):
    terminal_reason = None
    if disposition == 'acked':
        state, when = 'acked', now
    elif terminal:
        state, when = 'dead', now
        terminal_reason = reason
    else:
        rule = policy.read(db, item['channel'], item['policy'])
        if attempt['number'] >= rule['max_attempts']:
            state, when = 'dead', now
            terminal_reason = reason
        else:
            origin = attempt['expires'] if disposition == 'expired' else now
            when = origin + policy.delay(rule, attempt['number'])
            if delivery.stale(item, when):
                state, when = 'dead', now
                terminal_reason, kind = DEADLINE, 'stale'
            else:
                state = 'waiting'
    result = dict(delivery_id=item['id'], attempt=attempt['number'], outcome=disposition,
                  state=state, effective_at=when, reason=reason,
                  terminal_reason=terminal_reason)
    delivery.close(db, item['id'], state, when, terminal_reason)
    attempts.finish(db, attempt, disposition, reason, now, result)
    journal.append(db, item['channel'], now, kind or disposition, result, item['message_id'])
    return result
