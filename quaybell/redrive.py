from . import deadletters, delivery, journal, policy


def redrive(db, key, now):
    prior = deadletters.get(db, key)
    existing = db.execute('SELECT id,policy FROM deliveries WHERE parent_id=?', (key,)).fetchone()
    if existing:
        return {'source_id': key, 'delivery_id': existing['id'], 'policy': existing['policy'], 'repeated': True}
    active = policy.read(db, prior['channel'])
    new = delivery.add(db, prior['message_id'], prior['subscription_id'], prior['lane'], active['version'], now, key)
    result = {'source_id': key, 'delivery_id': new, 'policy': active['version'], 'repeated': False}
    journal.append(db, prior['channel'], now, 'redriven', result, prior['message_id'])
    return result
