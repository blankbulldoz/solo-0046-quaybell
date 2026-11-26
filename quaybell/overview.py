from collections import Counter

from . import catalog, deadletters, eligibility, inspection, policy, subscriptions


def board(db, channel, now):
    catalog.get(db, channel)
    from . import drain
    summary = []
    for group in subscriptions.for_channel(db, channel):
        counts, sample = Counter(), []
        for item in eligibility.explain(db, group, now):
            counts[item['cause']] += 1
            if len(sample) < 20:
                sample.append(item)
        drain_view = drain.read_only_summary(db, group, now)
        summary.append(group | {'counts': dict(counts), 'sample': sample,
                                'drain': drain_view,
                                'drain_round': drain_view['round'] if drain_view else 0})
        summary.append(group | {'counts': dict(counts), 'sample': sample})
    return {'channel': channel, 'observed_at': now, 'policy': policy.read(db, channel),
            'groups': summary, 'messages': inspection.recent(db, channel),
            'dead': deadletters.list_recent(db, channel)}
