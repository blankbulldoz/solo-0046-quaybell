from collections import Counter

from . import catalog, deadletters, eligibility, policy, review, subscriptions


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
    first = review.page(db, channel, now, limit=review.DEFAULT_LIMIT)
    return {'channel': channel, 'observed_at': now, 'policy': policy.read(db, channel),
            'groups': summary, 'messages': first['messages'], 'message_page': first['page'],
            'pruned': first['pruned'], 'dead': deadletters.list_recent(db, channel)}
