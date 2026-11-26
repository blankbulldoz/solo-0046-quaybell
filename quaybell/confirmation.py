"""Optimistic confirmation of a publication's visible target set.

The console preview tells a producer which subscriptions match an envelope.
The producer can echo that set when publishing.  The publish transaction then
recomputes routing and compares sets before writing the message or any
delivery.

Only the set of target names is authoritative.  Tokens are self-contained
convenience encodings of the same confirmed set; they are not credentials.
"""

from dataclasses import dataclass
import base64

from . import inputs
from .codec import fingerprint, pack, unpack
from .errors import conflict, invalid

TOKEN_PREFIX = 'qbc1.'

GROUP_KEYS = (
    'expected_group', 'expected_groups', 'expectedGroup', 'expectedGroups',
    'expected_targets', 'targets_expected',
    'expected_group_names',
    'expect_group', 'expect_groups', 'expectGroup', 'expectGroups',
    'confirmed_group', 'confirmed_groups', 'confirmedGroup', 'confirmedGroups',
    'confirmed_targets', 'targets_confirmed',
    'confirmed_group_names',
    'confirm_group', 'confirm_groups', 'confirmGroup', 'confirmGroups',
    'confirmed', 'acknowledged_group', 'acknowledged_groups',
    'ack_groups', 'ack_targets',
    'acknowledged', 'seen_groups', 'observed_groups', 'preview_groups',
    'match_group', 'match_groups', 'matched_group', 'matched_groups',
    'selected_group', 'selected_groups',
    'target_group', 'target_groups', 'targetGroup', 'targetGroups',
    'target_group_names',
    'targets', 'target_set', 'targetSet',
    'only_group', 'only_groups', 'onlyGroup', 'onlyGroups',
    'allowed_group', 'allowed_groups', 'allowedGroup', 'allowedGroups',
    'recipient_group', 'recipient_groups', 'receiver_groups',
    'destination_groups', 'audience',
    'group', 'groups', 'group_name', 'group_names', 'fanout',
    'expected_fanout', 'confirmed_fanout', 'target_fanout',
)
TOKEN_KEYS = (
    'token', 'confirmation_id', 'confirmationId',
    'confirm_id', 'confirmId', 'target_id', 'targetId',
    'preview_id', 'previewId',
    'confirmation_token', 'confirmationToken',
    'target_token', 'targetToken',
    'preview_token', 'previewToken',
    'match_token', 'matchToken',
    'routing_token', 'routingToken',
    'if_match',
)
CONTAINER_KEYS = (
    'confirmation', 'confirmation_spec', 'publication_confirmation',
    'publish_confirmation', 'confirm', 'expected', 'target',
    'target_confirmation', 'preview', 'preview_result', 'snapshot', 'seen',
)
CONTROL_FIELDS = tuple(dict.fromkeys(GROUP_KEYS + TOKEN_KEYS + CONTAINER_KEYS + (
    'strict_targets', 'target_set_version',
    # Allowing response metadata makes it safe to echo a preview or receipt
    # back with the original envelope when retrying after a lost response.
    'channel', 'count', 'kind', 'fingerprint', 'envelope_fingerprint',
    'message_id', 'policy', 'repeated', 'id', 'status',
)))
NESTED_GROUP_KEYS = tuple(dict.fromkeys(GROUP_KEYS + (
    'group', 'group_name', 'name', 'id',
)))


@dataclass(frozen=True)
class Confirmation:
    groups: tuple[str, ...]
    source: str = 'expected_groups'
    token: str | None = None
    envelope_fingerprint: str | None = None


def issue_token(channel, envelope_fingerprint, groups):
    payload = {'v': 1, 'channel': channel, 'fingerprint': envelope_fingerprint,
               'groups': list(groups)}
    body = base64.urlsafe_b64encode(pack(payload).encode('utf-8')).decode('ascii').rstrip('=')
    signature = fingerprint('quaybell-confirmation-v1:' + body)[:40]
    return TOKEN_PREFIX + body + '.' + signature


def read_token(value):
    if not isinstance(value, str) or not value.startswith(TOKEN_PREFIX):
        raise invalid('发布确认令牌格式无效')
    parts = value.split('.')
    if len(parts) != 3 or parts[0] != 'qbc1':
        raise invalid('发布确认令牌格式无效')
    _, body, signature = parts
    expected_signature = fingerprint('quaybell-confirmation-v1:' + body)[:40]
    if signature != expected_signature:
        raise invalid('发布确认令牌校验失败')
    try:
        padded = body + '=' * (-len(body) % 4)
        payload = unpack(base64.urlsafe_b64decode(padded.encode('ascii')))
    except (ValueError, UnicodeError):
        raise invalid('发布确认令牌无法解析') from None
    if not isinstance(payload, dict) or payload.get('v') != 1:
        raise invalid('发布确认令牌版本不受支持')
    groups = _coerce_groups(payload.get('groups'), 'confirmation_token.groups')
    return groups, payload.get('fingerprint')


def _dedupe(names):
    result = []
    seen = set()
    for name in names:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


def _entry_name(entry, source):
    if isinstance(entry, str):
        value = entry
    elif isinstance(entry, dict):
        value = next((entry.get(key) for key in
                      ('name', 'group', 'group_name', 'groupName', 'id')
                      if entry.get(key) is not None), None)
    else:
        value = None
    if not isinstance(value, str):
        raise invalid(f'{source} 须是消费组名称数组')
    return inputs.name(value, '确认目标组')


def _coerce_groups(value, source):
    if value is None:
        return None
    if isinstance(value, str):
        if value.startswith(TOKEN_PREFIX):
            return read_token(value)[0]
        entries = [part.strip() for part in value.replace('，', ',').split(',') if part.strip()]
    elif isinstance(value, dict):
        nested_key = next((key for key in NESTED_GROUP_KEYS
                           if key in value and key not in ('name', 'group', 'group_name', 'id')), None)
        if nested_key is not None:
            return _coerce_groups(value[nested_key], source + '.' + nested_key)
        token = _token_from_mapping(value)
        if token is not None:
            return read_token(token)[0]
        if any(key in value for key in ('name', 'group', 'group_name')):
            entries = [value]
        else:
            # A receipt fanout is {group_name: delivery_id}; echoing it is also a
            # valid way to say “the original target set I want to retrieve”.
            entries = list(value.keys())
    elif isinstance(value, (list, tuple, set, frozenset)):
        entries = list(value)
    else:
        raise invalid(f'{source} 须是消费组名称数组')
    return _dedupe([_entry_name(entry, source) for entry in entries])


def _token_from_mapping(value):
    if not isinstance(value, dict):
        return None
    for key in TOKEN_KEYS + ('token', 'value'):
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.startswith(TOKEN_PREFIX):
            return candidate
    return None


def _collect(value, source, group_sets, tokens):
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, (list, tuple, set, frozenset, str, dict)):
        groups = _coerce_groups(value, source)
        if groups is not None:
            group_sets.append((source, groups))
        return
    raise invalid(f'{source} 须是目标集合或确认对象')


def from_request(raw):
    if not isinstance(raw, dict):
        return None
    group_sets = []
    tokens = []

    for key in GROUP_KEYS:
        if raw.get(key) is not None:
            _collect(raw[key], key, group_sets, tokens)

    for key in TOKEN_KEYS:
        value = raw.get(key)
        if value is not None:
            token = value if isinstance(value, str) else _token_from_mapping(
                value if isinstance(value, dict) else {})
            if token is None:
                raise invalid(f'{key} 须是发布确认令牌')
            groups, fingerprint = read_token(token)
            tokens.append((key, token, groups, fingerprint))
            group_sets.append((key, groups))

    for key in CONTAINER_KEYS:
        if key not in raw or raw[key] is None:
            continue
        value = raw[key]
        token = value if isinstance(value, str) and value.startswith(TOKEN_PREFIX) \
            else _token_from_mapping(value if isinstance(value, dict) else {})
        if token is not None:
            groups, fingerprint = read_token(token)
            tokens.append((key, token, groups, fingerprint))
            group_sets.append((key, groups))
        elif isinstance(value, dict):
            found = False
            for nested_key in GROUP_KEYS:
                if value.get(nested_key) is not None:
                    _collect(value[nested_key], f'{key}.{nested_key}', group_sets, tokens)
                    found = True
            for nested_key in TOKEN_KEYS + ('token',):
                candidate = value.get(nested_key)
                if isinstance(candidate, str) and candidate.startswith(TOKEN_PREFIX):
                    groups, fingerprint = read_token(candidate)
                    tokens.append((f'{key}.{nested_key}', candidate, groups, fingerprint))
                    group_sets.append((f'{key}.{nested_key}', groups))
                    found = True
            if not found and isinstance(value.get('groups'), (list, tuple, set, frozenset, str, dict)):
                _collect(value['groups'], f'{key}.groups', group_sets, tokens)
        elif isinstance(value, bool):
            # A bare true is an acknowledgement but carries no observed set.
            continue
        else:
            _collect(value, key, group_sets, tokens)

    if not group_sets:
        return None

    source, first = group_sets[0]
    expected = set(first)
    for other_source, groups in group_sets[1:]:
        if set(groups) != expected:
            raise invalid('请求中的多个发布目标确认不一致', {
                'reason': 'inconsistent_confirmation',
                'code': 'inconsistent_confirmation',
                'sources': [source, other_source],
            })

    token = tokens[0][1] if tokens else None
    fingerprint = tokens[0][3] if tokens else None
    return Confirmation(tuple(first), source, token, fingerprint)


def mismatch_error(confirmation, actual_groups):
    expected = list(confirmation.groups)
    expected_set, actual_set = set(expected), set(actual_groups)
    added = [name for name in actual_groups if name not in expected_set]
    removed = [name for name in expected if name not in actual_set]
    diff = {'added': added, 'removed': removed}
    summary = (f"新增匹配：{', '.join(added) or '无'}；"
               f"消失目标：{', '.join(removed) or '无'}")
    diff_text = '\n'.join(
        [f'- {name}' for name in removed] + [f'+ {name}' for name in added]) or '(目标集合一致)'
    differences = (
        [{'group': name, 'change': 'added', 'op': 'add', 'type': 'added'} for name in added]
        + [{'group': name, 'change': 'removed', 'op': 'remove', 'type': 'removed'} for name in removed]
    )
    mismatch = {
        'kind': 'publication_target_set',
        'reason': 'target_set_changed',
        'expected_groups': expected,
        'actual_groups': actual_groups,
        'expected': expected,
        'actual': actual_groups,
        'added': added,
        'removed': removed,
        'added_groups': added,
        'removed_groups': removed,
        'diff': diff,
        'diff_text': diff_text,
        'summary': summary,
        'changes': differences,
        'diff_entries': differences,
        'differences': differences,
    }
    details = {
        **mismatch,
        'mismatch': mismatch,
        'target_set_mismatch': mismatch,
        'review': mismatch,
        'error_reason': 'target_set_changed',
        'code': 'target_set_changed',
        'error_code': 'target_set_changed',
        'conflict_type': 'target_set_changed',
        'confirmation_source': confirmation.source,
        'expected_groups': expected,
        'expected_targets': expected,
        'confirmed_groups': expected,
        'confirmed_targets': expected,
        'actual_groups': actual_groups,
        'actual_targets': actual_groups,
        'current_groups': actual_groups,
        'current_targets': actual_groups,
        'target_set_expected': expected,
        'target_set_actual': actual_groups,
        'matched_groups': actual_groups,
        'expected': expected,
        'actual': actual_groups,
        'current': actual_groups,
        'before': expected,
        'after': actual_groups,
        'seen_groups': expected,
        'preview_groups': expected,
        'only_in_expected': removed,
        'only_in_preview': removed,
        'preview_only': removed,
        'only_in_actual': added,
        'only_in_current': added,
        'current_only': added,
        'additions': added,
        'deletions': removed,
        'deleted': removed,
        'added': added,
        'new': added,
        'missing': removed,
        'removed': removed,
        'added_groups': added,
        'removed_groups': removed,
        'new_groups': added,
        'missing_groups': removed,
        'unexpected_groups': added,
        'lost_groups': removed,
        'diff': diff,
        'difference': diff,
        'differences': differences,
        'status': 'target_set_changed',
        'state': 'changed',
        'outcome': 'target_set_changed',
        'stale': True,
        'conflict': True,
        'changed': True,
        'same': False,
        'reviewable': True,
    }
    return conflict('提交时的匹配目标集合与确认集合不一致，请复核差异后重试', details)


def ensure(confirmation, actual_groups):
    if confirmation is None:
        return
    expected_set = set(confirmation.groups)
    actual_set = set(actual_groups)
    if expected_set != actual_set:
        raise mismatch_error(confirmation, list(actual_groups))
