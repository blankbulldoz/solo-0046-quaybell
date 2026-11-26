from collections import Counter

from . import catalog, deadletters, eligibility, inspection, policy, subscriptions


def board(db, channel, now):
    """看板只读路径：在同一读快照内逐组做全量积压归因。

    与 ``claims.claim`` 共享 ``eligibility`` 的同键队首定义，但这里不回收、
    不写入：越过截止尚未回收的队首由 ``eligibility.explain`` 记为
    ``expired``，而不是被就地处理。
    """
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
            'groups': summary, 'messages': inspection.recent(db, channel),
            'dead': deadletters.list_recent(db, channel)}
