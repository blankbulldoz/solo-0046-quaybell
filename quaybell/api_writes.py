from . import catalog, claims, disposition, expiry, inputs, publication, redrive, retention, subscriptions, takeover
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
    if action in ('takeover', 'handoff', 'handover', 'migrate', 'take_over'):
        item = takeover.create(db, args[0], args[1], raw, now)
        status = 200 if item.get('repeated') or item.get('preview') else 201
        return Reply(item, status)
    if action == 'channel_takeover':
        group = raw.get('group') or raw.get('source_group') or raw.get('old_group')
        item = takeover.create(db, args[0], group, raw, now)
        status = 200 if item.get('repeated') or item.get('preview') else 201
        return Reply(item, status)
    if action == 'takeover_plan':
        return Reply(takeover.preview(db, args[0], args[1], now, raw))
    if action == 'takeover_to_path':
        raw = dict(raw)
        raw.setdefault('new_group', args[2])
        item = takeover.create(db, args[0], args[1], raw, now)
        return Reply(item, 200 if item.get('repeated') or item.get('preview') else 201)
    if action == 'takeover_post_item':
        item = takeover.post_item(db, args[0], args[1], args[2], raw, now)
        return Reply(item, 200 if item.get('repeated') or item.get('preview') else 201)
    if action == 'takeover_retry':
        item = takeover.retry_route(db, args[0], args[1], args[2], raw, now)
        return Reply(item, 200)
    if action == 'takeover_transfer':
        item = takeover.transfer(db, args[0], args[1], raw, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    raise AssertionError(action)
