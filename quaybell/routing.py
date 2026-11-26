from . import catalog, subscriptions
from .envelope import Envelope


def accepts(tags, rules):
    return all(tags.get(key) == value for key, value in rules.items())


def matching(db, channel, tags):
    """Groups whose registered rule version accepts these tags."""
    return [dict(id=row['subscription_id'], name=row['group_name'], version=row['version'],
                 tags=row['tags'], paused=row['paused'])
            for row in subscriptions.rules_for(db, channel) if accepts(tags, row['tags'])]


def preview(db, channel, raw):
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw)
    groups = matching(db, channel, envelope.tags)
    return {'channel': channel, 'groups': [group['name'] for group in groups]}
