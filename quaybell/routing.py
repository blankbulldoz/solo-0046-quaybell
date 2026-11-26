from . import catalog, confirmation, subscriptions
from .envelope import Envelope


def matching(db, channel, tags):
    return [group for group in subscriptions.for_channel(db, channel)
            if all(tags.get(key) == value for key, value in group['tags'].items())]


def preview(db, channel, raw):
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw)
    groups = matching(db, channel, envelope.tags)
    names = [group['name'] for group in groups]
    fingerprint = envelope.fingerprint()
    token = confirmation.issue_token(channel, fingerprint, names)
    target = {
        'kind': 'publication_target_set',
        'channel': channel,
        'groups': names,
        'expected_groups': names,
        'target_groups': names,
        'targets': names,
        'count': len(names),
        'envelope_fingerprint': fingerprint,
        'fingerprint': fingerprint,
        'confirmation_token': token,
        'confirmation_id': token,
        'token': token,
    }
    return {
        'channel': channel,
        'groups': names,
        'matched_groups': names,
        'current_groups': names,
        'expected_groups': names,
        'target_groups': names,
        'targets': names,
        'count': len(names),
        'envelope_fingerprint': fingerprint,
        'fingerprint': fingerprint,
        'confirmation_token': token,
        'confirmation_id': token,
        'confirm_token': token,
        'preview_token': token,
        'match_token': token,
        'token': token,
        'confirmation': target,
    }
