from . import catalog, claims, clockwork, disposition, publication, subscriptions

CHANNEL = 'signal-weir'


def populate(persistence):
    with persistence.session(write=True) as db:
        if any(row['name'] == CHANNEL for row in catalog.list_all(db)):
            return
        now = clockwork.now_ms()
        rule = dict(lease_seconds=90, retry_seconds=20, max_attempts=1, retention_seconds=7200)
        catalog.create(db, {'name': CHANNEL, 'policy': rule}, now)
        subscriptions.create(db, CHANNEL, {'name': 'receipt-rotor'}, now)
        subscriptions.create(db, CHANNEL, {'name': 'drift-keeper', 'tags': {'feed': 'telemetry'}}, now)
        entries = [('coil-north', 'telemetry', 0), ('coil-north', 'telemetry', 0),
                   ('coil-south', 'telemetry', 540), ('coil-west', 'notices', 0)]
        for i, (lane, feed, delay) in enumerate(entries, 1):
            publication.publish(db, CHANNEL, dict(producer='pulse-encoder', key=f'pulse-{i:02}', lane=lane,
                                tags={'feed': feed}, payload={'reading': i * 12, 'unit': 'mV'},
                                delay_seconds=delay), now)
        taken = claims.claim(db, CHANNEL, 'receipt-rotor', {'worker': 'seed-worker'}, now)['items'][0]
        disposition.settle(db, taken['delivery_id'], {'token': taken['lease']['token'],
                           'reason': '下游解码器拒绝示范样本'}, now, 'nacked')
