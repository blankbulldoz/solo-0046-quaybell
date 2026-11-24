from . import catalog, subscriptions
from .envelope import Envelope


def matching(db, channel, tags):
    return [group for group in subscriptions.for_channel(db, channel)
            if all(tags.get(key) == value for key, value in group['tags'].items())]


def preview(db, channel, raw):
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw)
    groups = matching(db, channel, envelope.tags)
    return {'channel': channel, 'groups': [group['name'] for group in groups]}
