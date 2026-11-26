from . import attempts, delivery, failures, journal, policy


def apply(db, item, attempt, disposition, reason, now,
          classification=None, retry_after_seconds=None):
    terminal_reason = None
    if disposition == 'acked':
        state, when, classification = 'acked', now, None
    else:
        rule = policy.read(db, item['channel'], item['policy'])
        permanent = classification == failures.PERMANENT
        if permanent:
            # 明确永久拒绝（例如无法解析的消息）：不再消耗重试预算，立即死信。
            state, when = 'dead', now
            terminal_reason = failures.TERMINAL_PERMANENT
        elif attempt['number'] >= rule['max_attempts']:
            # 临时失败/超时仍按绑定策略退避；尝试预算用尽才终结。
            state, when = 'dead', now
            terminal_reason = failures.TERMINAL_BUDGET
        else:
            origin = attempt['expires'] if disposition == 'expired' else now
            if retry_after_seconds is not None:
                when = origin + retry_after_seconds
            else:
                when = origin + policy.delay(rule, attempt['number'])
            state = 'waiting'
        if classification is None:
            classification = (failures.TIMEOUT if disposition == 'expired' else failures.FAILED)
    result = dict(delivery_id=item['id'], attempt=attempt['number'], outcome=disposition,
                  state=state, effective_at=when, reason=reason,
                  policy=item['policy'], terminal_reason=terminal_reason)
    if classification is not None:
        result['classification'] = classification
    failures.annotate(result)
    delivery.close(db, item['id'], state, when, terminal_reason)
    attempts.finish(db, attempt, disposition, reason, now, result,
                    classification if disposition != 'acked' else None)
    journal.append(db, item['channel'], now, disposition, result, item['message_id'])
    return result
