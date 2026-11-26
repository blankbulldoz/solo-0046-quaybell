from . import catalog, claims, disposition, expiry, inputs, publication, redrive, retention, reviews, subscriptions
from .protocol import Reply


def write(db, action, args, raw, now):
    if action == 'create_channel':
        return Reply(catalog.create(db, raw, now), 201)
    if action == 'revision':
        return Reply(catalog.update_policy(db, *args, raw, now), 201)
    if action == 'subscribe':
        return Reply(subscriptions.create(db, *args, raw, now), 201)
    if action == 'pause':
        return Reply(subscriptions.pause(db, *args, raw, now))
    if action == 'publish':
        item = publication.publish(db, *args, raw, now)
        return Reply(item, 200 if item['repeated'] else 201)
    if action == 'resend_message':
        item = reviews.resend_message(db, int(args[0]), now)
        return Reply(item, 200)
    if action == 'candidate_create':
        return Reply(reviews.create_candidate(db, args[0], raw, now), 201)
    if action in ('candidate_confirm', 'candidate_confirm_global'):
        candidate_id = int(args[-1])
        item = reviews.confirm(db, candidate_id, raw, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    if action in ('candidate_discard', 'candidate_discard_global'):
        return Reply(reviews.discard(db, int(args[-1]), raw, now))
    if action == 'claim':
        return Reply(claims.claim(db, *args, raw, now))
    if action in ('ack', 'nack'):
        return Reply(disposition.settle(db, int(args[0]), raw, now, action + 'ed'))
    if action == 'sweep':
        return Reply(expiry.sweep(db, raw, now))
    if action == 'redrive':
        inputs.require(raw, ())
        item = redrive.redrive(db, int(args[0]), now)
        return Reply(item, 200 if item['repeated'] else 201)
    if action == 'prune':
        return Reply(retention.prune(db, *args, raw, now))
    raise AssertionError(action)
