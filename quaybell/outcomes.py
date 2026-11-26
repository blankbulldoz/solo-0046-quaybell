from . import attempts, delivery, journal, policy


def apply(db, item, attempt, disposition, reason, now):
    if disposition == 'acked':
        state, when = 'acked', now
    elif disposition == 'released':
        state, when = 'waiting', now
    else:
        rule = policy.read(db, item['channel'], item['policy'])
        if attempt['number'] >= rule['max_attempts']:
            state, when = 'dead', now
        else:
            origin = attempt['expires'] if disposition == 'expired' else now
            state, when = 'waiting', origin + policy.delay(rule, attempt['number'])
    result = dict(delivery_id=item['id'], attempt=attempt['number'], outcome=disposition,
                  state=state, effective_at=when, reason=reason)
    delivery.close(db, item['id'], state, when)
    attempts.finish(db, attempt, disposition, reason, now, result)
    journal.append(db, item['channel'], now, disposition, result, item['message_id'])
    return result
