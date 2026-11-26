from collections import Counter

from . import catalog, coalescing, deadletters, eligibility, inspection, policy, subscriptions


def board(db, channel, now):
    catalog.get(db, channel)
    summary = []
    for group in subscriptions.for_channel(db, channel):
        counts, sample = Counter(), []
        for item in eligibility.explain(db, group, now):
            counts[item['cause']] += 1
            if len(sample) < 20:
                sample.append(item)
        superseded_total = db.execute(
            "SELECT COUNT(*) FROM deliveries WHERE subscription_id=? AND state='superseded'",
            (group['id'],)).fetchone()[0]
        if superseded_total:
            counts['superseded'] = superseded_total
        summary.append(group | {'counts': dict(counts), 'sample': sample,
                                'superseded_total': superseded_total,
                                'coalesced_total': superseded_total})
    return {'channel': channel, 'observed_at': now, 'policy': policy.read(db, channel),
            'groups': summary, 'messages': inspection.recent(db, channel),
            'supersessions': coalescing.recent(db, channel),
            'dead': deadletters.list_recent(db, channel)}
