from . import catalog, claims, disposition, expiry, forensics, inputs, publication, redrive, retention, subscriptions
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
    if action == 'audit_config':
        return Reply(forensics.configure(db, args[0], raw, now), 201)
    if action == 'audit_write_entry':
        query_keys = {'message_id', 'id', 'audit_id', 'audit_period', 'period', 'range',
                      'window', 'producer', 'producer_id', 'limit', 'order', *forensics.KEY_FIELDS,
                      *forensics.TIME_FIELDS, *forensics.END_FIELDS}
        config_keys = {'enabled', 'retention_seconds', 'audit', *forensics.TTL_FIELDS}
        if raw.keys() & query_keys and not (raw.keys() & config_keys):
            return Reply(forensics.query(db, args[0], raw, now))
        return Reply(forensics.configure(db, args[0], raw, now), 201)
    if action == 'audit_query':
        return Reply(forensics.query(db, args[0], raw, now))
    if action == 'audit_verify':
        item = forensics.for_credential(db, int(args[0]), now)
        return Reply({'valid': item.get('digest_matches', False) and item.get('available', False),
                      'audit': item})
    raise AssertionError(action)
