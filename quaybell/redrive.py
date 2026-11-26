from . import deadletters, delivery, journal, outcomes, policy
from .errors import conflict


def redrive(db, key, now):
    prior = deadletters.get(db, key)
    existing = db.execute('SELECT id,policy FROM deliveries WHERE parent_id=?', (key,)).fetchone()
    if existing:
        return {'source_id': key, 'delivery_id': existing['id'], 'policy': existing['policy'], 'repeated': True}
    # A delivery closed by its message deadline is kept only for investigation;
    # redriving it would bypass the deadline the producer set.
    if prior['terminal_reason'] == outcomes.DEADLINE or delivery.stale(prior, now):
        raise conflict('原投递因超过处理期限进入死信，不可重投')
    active = policy.read(db, prior['channel'])
    new = delivery.add(db, prior['message_id'], prior['subscription_id'], prior['lane'], active['version'], now, key)
    result = {'source_id': key, 'delivery_id': new, 'policy': active['version'], 'repeated': False}
    journal.append(db, prior['channel'], now, 'redriven', result, prior['message_id'])
    return result
