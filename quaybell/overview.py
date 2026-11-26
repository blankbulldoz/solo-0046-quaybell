from collections import Counter

from . import assignments, catalog, deadletters, eligibility, inspection, policy, subscriptions


def board(db, channel, now):
    catalog.get(db, channel)
    channel_row = db.execute('SELECT partition_count FROM channels WHERE name=?', (channel,)).fetchone()
    summary = []
    for group in subscriptions.for_channel(db, channel):
        counts, sample = Counter(), []
        for item in eligibility.explain(db, group, now):
            counts[item['cause']] += 1
            if len(sample) < 20:
                sample.append(item)
        assignment = assignments.summary(db, channel, group['name'])
        if assignment.get('configured'):
            owner_by_partition = {item['partition']: item['owner'] for item in assignment['partitions']}
            count = assignment['partition_count']
            for item in sample:
                partition = assignments.partition_for(item['lane'], count)
                item['partition'] = partition
                item['owner'] = owner_by_partition.get(partition)
                item['assignment_version'] = assignment['version']
        summary.append(group | {'counts': dict(counts), 'sample': sample, 'assignment': assignment})
    channel_assignments = assignments.channel_view(db, channel, now)
    return {'channel': channel, 'observed_at': now, 'policy': policy.read(db, channel),
            'partition_count': channel_row['partition_count'],
            'partition_groups': channel_assignments['groups'],
            'groups': summary, 'messages': inspection.recent(db, channel),
            'dead': deadletters.list_recent(db, channel)}
