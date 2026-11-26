from . import catalog, claims, confluence_service, disposition, expiry, inputs, publication, redrive, retention, subscriptions
from .protocol import Reply


def write(db, action, args, raw, now):
    if action == 'create_channel':
        return Reply(catalog.create(db, raw, now), 201)
    if action == 'revision':
        return Reply(catalog.update_policy(db, *args, raw, now), 201)
    if action == 'subscribe':
        return Reply(subscriptions.create(db, *args, raw, now), 201)
    if action == 'group_update':
        if 'paused' in raw and len(raw) == 1:
            return Reply(subscriptions.pause(db, args[0], args[1], raw, now))
        if raw.get('mode') or raw.get('merge_mode') or raw.get('confluence') is not None or raw.get('merge') is not None:
            mode = raw.get('mode') or raw.get('merge_mode')
            if mode in ('all', 'ordinary', 'default', 'off', 'disabled') or raw.get('enabled') is False:
                return Reply(confluence_service.configure(
                    db, args[0], args[1], raw | {'enabled': False}, now))
            return Reply(confluence_service.configure(db, args[0], args[1], raw, now))
        from .errors import invalid
        raise invalid('消费组更新须包含 mode、merge_mode 或 paused')
    if action == 'group_mode':
        mode = raw.get('mode') or raw.get('merge_mode')
        if mode in ('confluence', 'merge', 'sequence', 'sequence_merge', 'source_sequence'):
            return Reply(confluence_service.configure(db, args[0], args[1], raw, now))
        if mode in ('all', 'ordinary', 'default', 'off', 'disabled'):
            return Reply(confluence_service.configure(
                db, args[0], args[1], raw | {'enabled': False}, now))
        # The baseline group has no other pluggable delivery mode; pause remains
        # a separate boolean.  Reject unknown values rather than silently no-op.
        from .errors import invalid
        raise invalid('不支持的消费组模式；合流模式请使用 confluence/sequence')
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
    if action == 'confluence_configure':
        return Reply(confluence_service.configure(db, args[0], args[1], raw or {}, now))
    if action == 'confluence_submit':
        return Reply(confluence_service.submit(db, args[0], args[1], raw or {}, now))
    if action == 'confluence_advance':
        return Reply(confluence_service.advance(db, args[0], args[1], raw or {}, now))
    if action == 'confluence_reconcile':
        return Reply(confluence_service.reconcile(db, args[0], args[1], int(args[2]), raw or {}, now))
    if action == 'confluence_upsert':
        if isinstance(raw, dict) and any(k in raw for k in
                                         ('source', 'source_id', 'chain', 'origin')) and any(
                                             k in raw for k in ('sequence', 'seq', 'number')):
            return Reply(confluence_service.submit(db, args[0], args[1], raw, now))
        return Reply(confluence_service.configure(db, args[0], args[1], raw or {}, now))
    if action == 'prune':
        return Reply(retention.prune(db, *args, raw, now))
    raise AssertionError(action)
