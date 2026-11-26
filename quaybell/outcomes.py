from . import attempts, delivery, journal, policy, reconcile


def apply(db, item, attempt, disposition, reason, now, result=None):
    if disposition == 'acked':
        state, when = 'acked', now
    else:
        rule = policy.read(db, item['channel'], item['policy'])
        if attempt['number'] >= rule['max_attempts']:
            state, when = 'dead', now
        else:
            origin = attempt['expires'] if disposition == 'expired' else now
            state, when = 'waiting', origin + policy.delay(rule, attempt['number'])
    result_view = reconcile.public_view(result) if result is not None else None
    receipt = dict(delivery_id=item['id'], attempt=attempt['number'], outcome=disposition,
                   state=state, effective_at=when, reason=reason)
    if result_view is not None:
        receipt['result'] = result_view
    delivery.close(db, item['id'], state, when)
    attempts.finish(db, attempt, disposition, reason, now, receipt, result)
    journal.append(db, item['channel'], now, disposition, receipt, item['message_id'])
    return receipt
