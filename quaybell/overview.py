from collections import Counter

from . import catalog, deadletters, eligibility, impact, inspection, policy, subscriptions


def board(db, channel, now):
    catalog.get(db, channel)
    summary = []
    for group in subscriptions.for_channel(db, channel):
        counts, sample = Counter(), []
        for item in eligibility.explain(db, group, now):
            counts[item['cause']] += 1
            if len(sample) < 20:
                sample.append(item)
        summary.append(group | {'counts': dict(counts), 'sample': sample})
    return {'channel': channel, 'observed_at': now, 'policy': policy.read(db, channel),
            'state_token': impact.state_token(db, channel),
            'groups': summary, 'messages': inspection.recent(db, channel),
            'dead': deadletters.list_recent(db, channel)}
