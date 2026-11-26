from . import (batches, catalog, claims, disposition, expiry, inputs, publication,
               redrive, retention, subscriptions)
from .protocol import Reply


def _batch_write(db, action, args, raw, now):
    if action == 'batch_create':
        item = batches.create(db, args[0], raw or {}, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_part':
        item = batches.put_part(db, args[0], args[1], raw or {}, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_part_at':
        item = batches.put_part(db, args[0], args[1], raw or {}, now, path_ref=args[2])
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_seal':
        item = batches.seal(db, args[0], args[1], raw or {}, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_cancel':
        return Reply(batches.cancel(db, args[0], args[1], raw or {}, now))
    if action == 'batch_receipt_write':
        return Reply(batches.get_receipt(db, args[0], args[1]))
    if action == 'batch_item_op':
        return Reply(batches.item_op(db, args[0], args[1], raw or {}, now))
    if action == 'batch_put':
        item = batches.put_item(db, args[0], args[1], raw or {}, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    # 顶层 /v1/batches/{ref}... 没有频道段，channel=None。
    if action == 'batch_item_op_global':
        return Reply(batches.item_op(db, None, args[0], raw or {}, now))
    if action == 'batch_put_global':
        item = batches.put_item(db, None, args[0], raw or {}, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_part_global':
        item = batches.put_part(db, None, args[0], raw or {}, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_part_at_global':
        item = batches.put_part(db, None, args[0], raw or {}, now, path_ref=args[1])
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_seal_global':
        item = batches.seal(db, None, args[0], raw or {}, now)
        return Reply(item, 200 if item.get('repeated') else 201)
    if action == 'batch_cancel_global':
        return Reply(batches.cancel(db, None, args[0], raw or {}, now))
    raise AssertionError(action)


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
    if action.startswith('batch_'):
        return _batch_write(db, action, args, raw, now)
    raise AssertionError(action)
