from collections import Counter

from . import catalog, deadletters, eligibility, inspection, policy, quota, subscriptions


def board(db, channel, now):
    catalog.get(db, channel)
    summary = []
    for group in subscriptions.for_channel(db, channel):
        counts, sample = Counter(), []
        for item in eligibility.explain(db, group, now):
            counts[item['cause']] += 1
            if len(sample) < 20:
                sample.append(item)
        summary.append(group | {'counts': dict(counts), 'sample': sample,
                                'quota': quota.status(db, group)})
    return {'channel': channel, 'observed_at': now, 'policy': policy.read(db, channel),
            'groups': summary, 'messages': inspection.recent(db, channel),
            'dead': deadletters.list_recent(db, channel)}
