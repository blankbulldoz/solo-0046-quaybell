import hmac
from math import isfinite
from urllib.parse import parse_qs

from . import codec, inspection
from .errors import invalid

TRACE_FORMAT = 'quaybell-trace/1'
WINDOW_FORMAT = 'quaybell-window-evidence/1'
HANDOFF_PROFILE = 'handoff-ten-minutes/1'
HANDOFF_WINDOW_SECONDS = 600

_START_KEYS = ('start', 'from', 'since', 'begin', 'start_at', 'start_time',
               'window_start', 'window_from', 'window_start_time')
_END_KEYS = ('end', 'to', 'until', 'finish', 'end_at', 'end_time',
             'window_end', 'window_to', 'window_end_time')
_WINDOW_SWITCH_KEYS = ('minutes', 'last_minutes', 'duration_seconds', 'preset', 'profile',
                       'handoff', 'at', 'handoff_at', 'center')

# 这些字段可能携带凭据或原始载荷；窗口导出只允许通过摘要提交它们。
_SCRUB_KEYS = frozenset((
    'token', 'lease_token', 'token_digest', 'receipt', 'payload', 'envelope',
    'raw_details', 'supplier_detail', 'result_digest',
))


def _sealed(data):
    body = {key: value for key, value in data.items() if key != 'sha256'}
    return body | {'sha256': codec.fingerprint(codec.pack(body))}


def export_trace(db, message_id, now, start=None, end=None):
    if start is not None or end is not None:
        if start is None or end is None:
            raise invalid('时间窗须同时提供 start 和 end')
        return export_window(db, message_id, now,
                             _epoch_second(start, '窗口起点'),
                             _epoch_second(end, '窗口终点'))
    facts = inspection.trace(db, message_id)
    data = dict(format=TRACE_FORMAT, observed_at=now, trace=facts)
    data['sha256'] = codec.fingerprint(codec.pack(data))
    return (codec.pack(data) + '\n').encode('ascii')


def _query_values(query):
    if isinstance(query, str):
        return parse_qs(query, keep_blank_values=True)
    if isinstance(query, dict):
        return query
    return {}


def _first_query(query, names):
    values = _query_values(query)
    for name in names:
        if name not in values:
            continue
        value = values[name]
        if isinstance(value, (list, tuple)):
            if len(value) != 1:
                raise invalid(f'查询参数 {name} 只能提供一次')
            value = value[0]
        return value
    return None


def _epoch_second(value, label):
    if isinstance(value, bool):
        raise invalid(f'{label} 必须是 Unix 秒')
    if isinstance(value, int):
        number = value
    elif isinstance(value, float) and isfinite(value):
        number = value
    elif isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            raise invalid(f'{label} 必须是 Unix 秒') from None
        if not isfinite(number):
            raise invalid(f'{label} 必须是有限 Unix 秒')
        if number.is_integer():
            number = int(number)
    else:
        raise invalid(f'{label} 必须是 Unix 秒')
    return number


def _duration_seconds(query, now):
    raw = _first_query(query, ('duration_seconds',))
    if raw is not None:
        value = _epoch_second(raw, '窗口长度')
        if value <= 0 or value > HANDOFF_WINDOW_SECONDS:
            raise invalid(f'窗口长度须为 1～{HANDOFF_WINDOW_SECONDS} 秒')
        return value
    raw = _first_query(query, ('minutes', 'last_minutes'))
    if raw is not None:
        value = _epoch_second(raw, '窗口分钟数')
        if value <= 0 or value > 10:
            raise invalid('交班窗口须为 0～10 分钟（不含 0）')
        return int(value * 60)
    return HANDOFF_WINDOW_SECONDS


def window_from_query(query, now):
    """解析显式时间窗；没有任何窗口参数时返回 None，以保持整包导出兼容。"""
    if query is None or query == '':
        return None
    values = _query_values(query)
    known = set(_START_KEYS + _END_KEYS + _WINDOW_SWITCH_KEYS)
    if not any(key in known or key in ('window',) for key in values):
        return None

    preset = _first_query(query, ('preset', 'profile', 'window'))
    start_raw = _first_query(query, _START_KEYS)
    end_raw = _first_query(query, _END_KEYS)
    at_raw = _first_query(query, ('at', 'handoff_at', 'center'))
    duration = _duration_seconds(query, now)

    if preset in ('handoff', 'handoff-10m', HANDOFF_PROFILE):
        if end_raw is None and start_raw is None:
            end_raw = now
        elif end_raw is None:
            pass
    if _first_query(query, ('last_minutes', 'minutes')) is not None \
            and start_raw is None and end_raw is None and at_raw is None:
        end_raw = now
    if at_raw is not None and start_raw is None and end_raw is None:
        center = _epoch_second(at_raw, '交班时刻')
        start_raw = center - duration // 2
        end_raw = start_raw + duration
    if start_raw is not None and end_raw is None:
        start = _epoch_second(start_raw, '窗口起点')
        end = start + duration
    elif end_raw is not None and start_raw is None:
        end = _epoch_second(end_raw, '窗口终点')
        start = end - duration
    elif start_raw is not None and end_raw is not None:
        start = _epoch_second(start_raw, '窗口起点')
        end = _epoch_second(end_raw, '窗口终点')
    else:
        raise invalid('时间窗须提供 start/end，或提供 at / last_minutes')

    if start > end:
        raise invalid('窗口起点不能晚于终点')
    if end - start > HANDOFF_WINDOW_SECONDS:
        raise invalid(f'事故审计窗口不能超过 {HANDOFF_WINDOW_SECONDS} 秒')
    return {'start': start, 'end': end}


def _relation(moment, start, end):
    if moment < start:
        return 'before_window'
    if moment > end:
        return 'after_window'
    return 'in_window'


def _scrub(value):
    if isinstance(value, dict):
        return {key: _scrub(item) for key, item in value.items()
                if key not in _SCRUB_KEYS and not key.endswith('_token')}
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    return value


def _source_commitment(event):
    return 'sha256:' + codec.fingerprint(codec.pack(event))


def _has_reclamation(entries, delivery_id, attempt_number):
    return any(event.get('kind') == 'expired'
               and event.get('details', {}).get('delivery_id') == delivery_id
               and event.get('details', {}).get('attempt') == attempt_number
               for event in entries)


def _delivery_context(facts, start, end, observed_at, entries):
    result = []
    publication_relation = _relation(facts['message']['published'], start, end)
    for delivery in facts['deliveries']:
        ready_relation = _relation(delivery['ready_at'], start, end)
        attempts = []
        for attempt in delivery['attempts']:
            started_relation = _relation(attempt['started'], start, end)
            expires_relation = _relation(attempt['expires'], start, end)
            item = {'number': attempt['number'], 'started_relation': started_relation,
                    'expires_relation': expires_relation}
            if started_relation == 'in_window':
                item['started_at'] = attempt['started']
                item['worker'] = attempt['worker']
            # 领取动作本身在窗内时，租约截止时间是该窗内授权条款的一部分。
            if expires_relation == 'in_window' or started_relation == 'in_window':
                item['expires_at'] = attempt['expires']
            if attempt['finished'] is None:
                item['completion'] = 'not_recorded'
                if attempt['outcome'] == 'open':
                    item['recorded_outcome'] = 'open'
            else:
                finished_relation = _relation(attempt['finished'], start, end)
                item['completion_relation'] = finished_relation
                if finished_relation == 'in_window':
                    item['finished_at'] = attempt['finished']
                    item['outcome'] = attempt['outcome']
                    if attempt['reason'] is not None:
                        item['reason'] = attempt['reason']
            attempts.append(item)
        result.append({
            'delivery_id': delivery['id'], 'group': delivery['group_name'],
            'policy_version': delivery['policy'], 'parent_id': delivery['parent_id'],
            'creation_relation': publication_relation,
            'ready_relation': ready_relation,
            'ready_at': delivery['ready_at'] if ready_relation == 'in_window' else None,
            'attempts': attempts,
        })
        # None is canonicalized but still emitted if not removed by JSON? Keep dict clean.
        if result[-1]['ready_at'] is None:
            result[-1].pop('ready_at')
    return result


def build_window(facts, observed_at, start, end):
    """构造选择性披露的时间窗证据；窗外记录只留关系和摘要承诺。"""
    entries = []
    outside_facts = []

    for event in facts['events']:
        relation = _relation(event['at'], start, end)
        if relation == 'in_window':
            safe_event = dict(event)
            safe_event['details'] = _scrub(event.get('details', {}))
            commitment = _source_commitment(safe_event)
            entry = dict(safe_event)
            entry.update(occurred_at=event['at'], time_basis='journal.at',
                         window_relation='in_window', source_commitment=commitment)
            entries.append(entry)
        else:
            commitment = _source_commitment(event)
            outside_facts.append({
                'kind': event['kind'], 'source': f"journal:{event['id']}",
                'time_relation': relation, 'commitment': commitment,
            })

    # “租约截止”是由 attempts.expires 给出的时间事实；真正回收发生在 journal.expired.at。
    deadline_entries = []
    for delivery in facts['deliveries']:
        for attempt in delivery['attempts']:
            expires = attempt['expires']
            if _relation(expires, start, end) != 'in_window' or expires > observed_at:
                continue
            closed_early = (attempt['outcome'] in ('acked', 'nacked')
                            and attempt['finished'] is not None
                            and attempt['finished'] < expires)
            if closed_early:
                continue
            reclaimed_in_window = _has_reclamation(entries, delivery['id'], attempt['number'])
            details = {
                'delivery_id': delivery['id'], 'attempt': attempt['number'],
                'expires_at': expires,
                'deadline_rule': 'attempts.expires 是凭据失效截止；到期不等于回收事务已发生',
                'reclamation_recorded_in_window': reclaimed_in_window,
            }
            started_relation = _relation(attempt['started'], start, end)
            if started_relation == 'in_window':
                details['worker'] = attempt['worker']
            deadline_entries.append({
                'id': f"lease-deadline:{delivery['id']}:{attempt['number']}",
                'kind': 'lease_deadline_reached', 'at': expires, 'occurred_at': expires,
                'time_basis': 'attempts.expires', 'window_relation': 'in_window',
                'details': details,
            })

    entries.extend(deadline_entries)
    entries.sort(key=lambda item: (item['at'], 0 if item['kind'] == 'lease_deadline_reached' else 1,
                                   item['id'] if isinstance(item['id'], int) else 10**18))

    message = facts['message']
    publication_relation = _relation(message['published'], start, end)
    subject = {
        'message_id': message['id'], 'channel': message['channel'],
        'policy_version': message['policy'],
        'envelope_commitment': 'sha256:' + message['fingerprint'],
        'published_relation': publication_relation,
    }
    if publication_relation == 'in_window':
        subject.update(published_at=message['published'], producer=message['producer'],
                       dedup_key=message['dedup_key'], lane=message['lane'],
                       envelope=message['envelope'])

    trace_body = dict(format=TRACE_FORMAT, observed_at=observed_at, trace=facts)
    source = {
        'format': TRACE_FORMAT,
        'trace_commitment': 'sha256:' + codec.fingerprint(codec.pack(facts)),
        'evidence_commitment': 'sha256:' + codec.fingerprint(codec.pack(trace_body)),
    }
    window = {
        'profile': HANDOFF_PROFILE,
        'start': start, 'end': end, 'duration_seconds': end - start,
        'boundary': 'closed_interval_start_and_end_included',
        'max_duration_seconds': HANDOFF_WINDOW_SECONDS,
        'occurrence_basis': {
            'journal_event': 'journal.at；这是发布、领取、确认、拒绝和实际回收的记录时刻',
            'lease_deadline_reached': 'attempts.expires；它是凭据失效口径，不代表回收事务已经发生',
        },
    }
    return {
        'format': WINDOW_FORMAT,
        'observed_at': observed_at,
        'window': window,
        'subject': subject,
        'trace': {
            'message': subject,
            'deliveries': _delivery_context(facts, start, end, observed_at, entries),
            'events': entries,
        },
        'outside_facts': outside_facts,
        'source': source,
    }


def export_window(db, message_id, now, start, end):
    if end - start > HANDOFF_WINDOW_SECONDS:
        raise invalid(f'事故审计窗口不能超过 {HANDOFF_WINDOW_SECONDS} 秒')
    facts = inspection.trace(db, message_id)
    document = _sealed(build_window(facts, now, start, end))
    return (codec.pack(document) + '\n').encode('ascii')


# 兼容不同调用方对该能力的命名。
export_windowed_trace = export_window
export_time_window = export_window
export_window_evidence = export_window


def _finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return isfinite(value)


def verify_document(document, expected_source_commitment=None, expected_sha256=None):
    """只凭窗口件复核摘要、时间边界，并给出该窗口能够支持的结论。"""
    result = {
        'ok': False, 'valid': False, 'format': None, 'integrity_valid': False,
        'time_scope_valid': False, 'errors': [], 'warnings': [], 'reason': None,
        'conclusions': [], 'supported': [], 'not_supported': [],
    }
    if not isinstance(document, dict):
        result['errors'].append({'code': 'document_not_object', 'message': '证据文件不是 JSON 对象'})
        return result

    result['format'] = document.get('format')
    claimed = document.get('sha256')
    body = {key: value for key, value in document.items() if key != 'sha256'}
    try:
        actual = codec.fingerprint(codec.pack(body))
    except (TypeError, ValueError):
        result['errors'].append({'code': 'canonical_json_invalid',
                                 'message': '文件包含无法规范化摘要的 JSON 值'})
        result['reason'] = result['errors'][0]['message']
        return result
    if isinstance(claimed, str) and hmac.compare_digest(claimed, actual):
        result['integrity_valid'] = True
    else:
        result['errors'].append({
            'code': 'digest_mismatch',
            'message': 'sha256 与规范化内容不符；文件已被改动或复制不完整',
            'claimed_sha256': claimed if isinstance(claimed, str) else None,
            'recomputed_sha256': actual,
        })
    result['sha256'] = claimed if isinstance(claimed, str) else None
    result['recomputed_sha256'] = actual

    if expected_sha256 is not None:
        expected_digest = str(expected_sha256).removeprefix('sha256:')
        if not hmac.compare_digest(actual, expected_digest):
            result['errors'].append({
                'code': 'pinned_digest_mismatch',
                'message': '重算摘要与通过独立渠道钉住的窗口件摘要不一致；即使文件内 sha256 被重算，也不能冒充原件',
                'pinned_sha256': expected_digest,
                'recomputed_sha256': actual,
            })

    window = document.get('window')
    if result['format'] != WINDOW_FORMAT and not isinstance(window, dict):
        # 兼容既有 quaybell-trace/1、handoff、compare 等自摘要文件。
        result['time_scope_valid'] = None
        result['ok'] = result['integrity_valid'] and not result['errors']
        result['valid'] = result['ok']
        result['reason'] = result['errors'][0]['message'] if result['errors'] else None
        return result

    time_errors = []
    integrity_errors = []
    if not isinstance(window, dict):
        time_errors.append({'code': 'window_missing', 'message': '窗口件缺少 window 定义'})
        start = end = None
    else:
        start, end = window.get('start'), window.get('end')
        if not _finite_number(start) or not _finite_number(end):
            time_errors.append({'code': 'window_bounds_invalid', 'message': 'window.start/end 必须是有限 Unix 秒'})
        elif start > end:
            time_errors.append({'code': 'window_bounds_invalid', 'message': 'window.start 不能晚于 end'})
        else:
            duration = window.get('duration_seconds')
            if not _finite_number(duration) or abs(duration - (end - start)) > 1e-9:
                time_errors.append({'code': 'window_duration_mismatch', 'message': 'duration_seconds 与 start/end 不一致'})
            max_duration = window.get('max_duration_seconds', HANDOFF_WINDOW_SECONDS)
            if _finite_number(max_duration) and end - start > max_duration:
                time_errors.append({'code': 'window_too_large', 'message': '时间窗超过授权的交班十分钟口径'})

    trace = document.get('trace') if isinstance(document.get('trace'), dict) else {}
    events = trace.get('events', [])
    if not isinstance(events, list):
        time_errors.append({'code': 'events_invalid', 'message': 'trace.events 必须是数组'})
        events = []

    if start is not None and end is not None and _finite_number(start) and _finite_number(end):
        for index, event in enumerate(events):
            if not isinstance(event, dict):
                time_errors.append({'code': 'event_not_object', 'path': f'trace.events[{index}]'})
                continue
            moment = event.get('at', event.get('occurred_at'))
            path = f'trace.events[{index}]'
            if not _finite_number(moment):
                time_errors.append({'code': 'event_time_invalid', 'path': path,
                                    'message': '事件缺少有限的 at/occurred_at'})
                continue
            if 'occurred_at' in event and event['occurred_at'] != event.get('at'):
                time_errors.append({'code': 'event_time_basis_mismatch', 'path': path,
                                    'message': 'at 与 occurred_at 不一致'})
            source_commitment = event.get('source_commitment')
            if isinstance(source_commitment, str) and source_commitment.startswith('sha256:') \
                    and {'id', 'at', 'kind', 'details'} <= event.keys():
                source_record = {key: event[key] for key in ('id', 'at', 'kind', 'details')}
                recomputed_commitment = 'sha256:' + codec.fingerprint(codec.pack(source_record))
                if not hmac.compare_digest(source_commitment, recomputed_commitment):
                    integrity_errors.append({
                        'code': 'source_commitment_mismatch', 'path': path,
                        'kind': event.get('kind'),
                        'message': '窗内事件内容与其 source_commitment 不符；事实可能被改动后只重算了外层摘要',
                    })
            relation = _relation(moment, start, end)
            if relation != 'in_window':
                time_errors.append({
                    'code': 'event_outside_window', 'path': path,
                    'kind': event.get('kind'), 'occurred_at': moment,
                    'time_relation': relation,
                    'message': '事件真实时间落在声明窗口外，不能作为窗内事实提交',
                })
            if event.get('window_relation') not in (None, 'in_window'):
                time_errors.append({
                    'code': 'outside_event_mislabeled', 'path': path,
                    'kind': event.get('kind'),
                    'message': '事件的 window_relation 未声明为 in_window',
                })

        for index, marker in enumerate(document.get('outside_facts', [])):
            if not isinstance(marker, dict):
                continue
            relation = marker.get('time_relation')
            if relation == 'in_window':
                time_errors.append({
                    'code': 'outside_fact_claimed_inside',
                    'path': f'outside_facts[{index}]', 'kind': marker.get('kind'),
                    'message': '该记录按时间口径属于窗外，却被改标为窗内；窗内事实必须进入 trace.events',
                })
            elif relation not in ('before_window', 'after_window'):
                time_errors.append({
                    'code': 'outside_relation_invalid', 'path': f'outside_facts[{index}]',
                    'message': '窗外摘要只允许 before_window/after_window 关系',
                })

    result['errors'].extend(time_errors)
    result['errors'].extend(integrity_errors)
    result['time_scope_valid'] = not time_errors
    if integrity_errors:
        result['integrity_valid'] = False

    source = document.get('source') if isinstance(document.get('source'), dict) else {}
    source_commitment = source.get('evidence_commitment')
    result['source_commitment'] = source_commitment
    if expected_source_commitment is not None:
        expected = expected_source_commitment
        if not str(expected).startswith('sha256:'):
            expected = 'sha256:' + str(expected)
        if source_commitment != expected:
            result['errors'].append({'code': 'source_pin_mismatch',
                                     'message': '窗口件绑定的原证据摘要与外部钉住值不一致'})

    kinds = [event.get('kind') for event in events if isinstance(event, dict)]
    counts = {kind: kinds.count(kind) for kind in sorted(set(kinds)) if kind is not None}
    result['event_counts'] = counts
    claims = {
        'published_in_window': 'published' in counts,
        'claimed_in_window': 'leased' in counts,
        'acknowledged_in_window': 'acked' in counts,
        'nacked_in_window': 'nacked' in counts,
        'actually_reclaimed_in_window': 'expired' in counts,
        'lease_deadline_reached_in_window': 'lease_deadline_reached' in counts,
    }
    result['claims'] = claims

    supported = []
    not_supported = []
    if claims['claimed_in_window']:
        supported.append(f"窗口内有 {counts['leased']} 条领取（leased）记录")
    if claims['acknowledged_in_window']:
        supported.append('窗口内有确认（acked）记录，可证明该确认动作发生在窗口时间口径内')
    if claims['actually_reclaimed_in_window']:
        supported.append('窗口内有 expired 处置记录，可证明租约是在该窗口内被实际回收/结算')
    if claims['lease_deadline_reached_in_window']:
        supported.append('窗口内有租约截止时间到达的时间事实')
        for event in events:
            if event.get('kind') != 'lease_deadline_reached':
                continue
            details = event.get('details', {})
            if not details.get('reclamation_recorded_in_window'):
                not_supported.append(
                    f"投递 #{details.get('delivery_id')} 第 {details.get('attempt')} 次租约："
                    '只能证明截止时间落在窗口内，不能证明已在窗口内实际回收'
                )
    if claims['published_in_window']:
        supported.append('窗口内有发布记录')
    else:
        not_supported.append('窗口内没有发布记录；不能据此主张消息发布发生在该窗口内')
    if not claims['acknowledged_in_window']:
        not_supported.append('窗口内没有确认记录；不能据此主张确认发生在该窗口内')
    if not claims['actually_reclaimed_in_window']:
        not_supported.append('窗口内没有 expired 实际回收记录；租约到期时间不等于实际回收时间')
    result['supported'] = supported
    result['not_supported'] = not_supported
    result['conclusions'] = supported + [f'不能支持：{item}' for item in not_supported]
    result['ok'] = result['integrity_valid'] and result['time_scope_valid'] and not result['errors']
    result['valid'] = result['ok']
    result['reason'] = result['errors'][0]['message'] if result['errors'] else None
    return result


verify_evidence = verify_document
verify = verify_document
