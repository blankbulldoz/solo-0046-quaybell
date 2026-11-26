from collections import Counter

from . import catalog, confluence_service, deadletters, eligibility, inspection, policy, subscriptions


def board(db, channel, now):
    catalog.get(db, channel)
    summary = []
    for group in subscriptions.for_channel(db, channel):
        counts, sample = Counter(), []
        for item in eligibility.explain(db, group, now):
            counts[item['cause']] += 1
            if len(sample) < 20:
                sample.append(item)
        group_summary = group | {'counts': dict(counts), 'sample': sample}
        if confluence_service.is_enabled(db, group['id']):
            merge = confluence_service.snapshot(db, channel, group['name'], now)
            group_summary['merge_mode'] = 'confluence'
            group_summary['confluence_mode'] = 'sequence'
            group_summary['confluence'] = {
                'enabled': True,
                'mode': merge['mode'],
                'time_budget_seconds': merge['time_budget_seconds'],
                'gaps': merge['gaps'],
                'missing_sequences': merge['missing_sequences'],
                'counts': merge['counts'],
                'late_arrivals': merge['late_arrivals'][-10:],
                'incidents': merge['incidents'][-10:],
                'snapshot_url': f'/v1/channels/{channel}/groups/{group["name"]}/confluence',
            }
        summary.append(group_summary)
    return {'channel': channel, 'observed_at': now, 'policy': policy.read(db, channel),
            'groups': summary, 'messages': inspection.recent(db, channel),
            'dead': deadletters.list_recent(db, channel)}
