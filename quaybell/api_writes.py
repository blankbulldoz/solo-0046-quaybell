from . import (assignments, catalog, claims, disposition, expiry, inputs, publication,
               redrive, retention, subscriptions)
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
    if action == 'configure_assignment':
        return Reply(assignments.configure(db, args[0], args[1], raw, now))
    if action == 'rebalance':
        return Reply(assignments.configure(db, args[0], args[1], raw or {}, now, rebalance=True))
    if action == 'fence_leases':
        return Reply(assignments.fence_now(db, args[0], args[1], raw or {}, now))
    if action == 'authorize':
        return Reply(assignments.authorize(db, args[0], args[1], raw or {}, now))
    if action == 'join':
        return Reply(assignments.join(db, args[0], args[1], raw or {}, now))
    if action == 'member_touch':
        return Reply(assignments.touch_from_body(db, args[0], args[1], raw or {}, now))
    if action == 'member_leave':
        return Reply(assignments.leave_from_body(db, args[0], args[1], raw or {}))
    if action == 'member_heartbeat_path':
        return Reply(assignments.touch_member(db, args[0], args[1], args[2], now))
    if action == 'member_leave_path':
        return Reply(assignments.leave(db, args[0], args[1], args[2]))
    if action == 'channel_configure':
        return Reply(assignments.configure_channel(db, args[0], raw or {}, now))
    raise AssertionError(action)
