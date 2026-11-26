from . import archive, catalog, claims, disposition, expiry, inputs, publication, redrive, retention, subscriptions
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
    if action == 'open_backfill':
        item = archive.create(db, args[0], args[1], raw, now)
        return Reply(item, 200 if item['repeated'] else 201)
    if action == 'intake_evidence':
        item = archive.intake(db, args[0], raw, now)
        return Reply(item, 200 if item['repeated'] else 201)
    if action == 'resolve_gap':
        return Reply(archive.resolve(db, int(args[1]), raw, now))
    raise AssertionError(action)
