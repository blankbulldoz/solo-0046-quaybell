"""两组对照调查：同一消息下两个消费组的状态、尝试路径与身份代际。

读模型只依赖既有表和日志，不产生任何写入。它额外回答三件值守最关心的事：

* ``not_dispatched``：发布时刻该组没有分发（发布后才建组，或当时标签不匹配）；
* ``waiting``：分发存在但当前处于等待（退避、延迟、顺序阻挡或暂停）；
* ``redrive_successor``：终败重投追加的后继投递是一个新的投递身份。

观察时刻 ``observed_at`` 与摘要由 :mod:`quaybell.evidence` 绑定，导出或共享的
快照在后续尝试发生后仍可复核它冻结的是哪个时刻。
"""
import json

from . import attempts, eligibility, publication, subscriptions
from .errors import invalid

FIRST_KEYS = ('a', 'left', 'group_a', 'left_group', 'g1', 'x', 'first')
SECOND_KEYS = ('b', 'right', 'group_b', 'right_group', 'g2', 'y', 'second')
PAIR_KEYS = ('groups', 'group', 'pair', 'compare')

STATUS_LABELS = {
    'unknown_group': '消费组不存在',
    'not_dispatched': '当时没有分发',
    'waiting': '等待中',
    'leased': '处理中（租约有效）',
    'acked': '已确认',
    'dead': '终败（死信）',
}

ABSENCE_LABELS = {
    'group_created_after_publication': '该组在消息发布之后才建立，发布时刻不存在，因此当时没有分发',
    'tags_did_not_match': '该组在发布时已存在，但标签与信封不匹配，因此当时没有分发',
    'group_unknown': '当前拓扑中也找不到这个消费组',
}

WAIT_LABELS = {
    'retry_backoff': '失败后退避，等待下一次尝试',
    'retry_ready': '退避已结束，等待再次领取',
    'delayed': '未到就绪时间',
    'blocked': '被同一顺序键的更早投递阻挡',
    'paused': '消费组已暂停取件',
    'ready': '已就绪，等待值守员或工作进程领取',
    'leased': '租约处理中',
    'expired': '租约已超时，等待回收',
}


def _first(query, keys):
    for key in keys:
        values = query.get(key) if query else None
        if values:
            return values[0]
    return None


def _split_pair_value(value):
    if isinstance(value, (list, tuple)):
        items = []
        for chunk in value:
            items.extend(str(chunk).split(','))
        return [item.strip() for item in items if str(item).strip()]
    return [item.strip() for item in str(value).split(',') if item.strip()]


def pair_from_query(query):
    """从常见查询参数写法中取出两个组名（兼容 a/b、left/right、groups=a,b）。"""
    first = _first(query, FIRST_KEYS)
    second = _first(query, SECOND_KEYS)
    if first is not None and second is not None:
        return str(first).strip(), str(second).strip()
    paired = _first(query, PAIR_KEYS)
    if paired is not None:
        values = _split_pair_value(query.get(PAIR_KEYS[0]))
        if len(values) >= 2:
            return values[0], values[1]
    values = query.get('group') if query else None
    if values and len(values) >= 2:
        return values[0], values[1]
    return first, second


def pair_from_body(raw):
    if not isinstance(raw, dict):
        return None, None
    for left_key, right_key in zip(FIRST_KEYS[:3], SECOND_KEYS[:3]):
        if raw.get(left_key) is not None and raw.get(right_key) is not None:
            return str(raw[left_key]), str(raw[right_key])
    groups = raw.get('groups') or raw.get('pair')
    if isinstance(groups, (list, tuple)) and len(groups) >= 2:
        return str(groups[0]), str(groups[1])
    return None, None


def require_pair(first, second):
    if not first or not second:
        raise invalid('对照需要指定两个消费组（a 与 b，或 groups=甲组,乙组）')
    if str(first) == str(second):
        raise invalid('请选择两个不同的消费组进行对照')
    return str(first), str(second)


def _publication_event(db, message_id):
    return db.execute("""SELECT id,at FROM journal WHERE message_id=? AND kind='published'
                         ORDER BY id LIMIT 1""", (message_id,)).fetchone()


def _subscription_events(db, channel):
    rows = db.execute("""SELECT id,at,details FROM journal
                         WHERE channel=? AND kind='subscribed' ORDER BY id""", (channel,)).fetchall()
    events = {}
    for row in rows:
        details = json.loads(row['details'])
        events[details.get('group')] = {'event_id': row['id'], 'at': row['at']}
    return events


def _redrive_events(db, message_id):
    rows = db.execute("""SELECT at,details FROM journal
                         WHERE message_id=? AND kind='redriven' ORDER BY id""", (message_id,)).fetchall()
    by_delivery = {}
    for row in rows:
        details = json.loads(row['details'])
        if details.get('delivery_id') is not None:
            by_delivery[details['delivery_id']] = row['at']
    return by_delivery


def resolve_group(db, channel, identifier):
    """组名优先；也接受订阅 id 的数字写法。找不到时返回 None。"""
    group = subscriptions.for_channel(db, channel)
    for item in group:
        if item['name'] == identifier:
            return item
    if isinstance(identifier, int) or (isinstance(identifier, str) and identifier.isdigit()):
        target = int(identifier)
        for item in group:
            if item['id'] == target:
                return item
    return None


def _attempt_path(deliveries, redrive_times, published_at):
    """按投递代际展开的尝试路径：身份节点与其各次尝试交替有序。"""
    path = []
    for item in deliveries:
        successor = item['parent_id'] is not None
        path.append({
            'kind': 'redrive_identity' if successor else 'original_identity',
            'at': redrive_times.get(item['id']) if successor else published_at,
            'delivery_id': item['id'],
            'parent_id': item['parent_id'],
            'identity': 'redrive_successor' if successor else 'original',
            'new_identity': successor,
            'state': item['state'],
        })
        for attempt in item['attempts']:
            path.append({'kind': 'attempt', **attempt})
    return path


def _waiting_kind(tip, cause_row):
    cause = cause_row['cause'] if cause_row else 'waiting'
    failed_before = any(a['outcome'] in ('nacked', 'expired') for a in tip['attempts'])
    if cause in ('delayed', 'ready') and failed_before and tip['state'] == 'waiting':
        # 退避中的就绪时间一定在未来；到点后 cause 会变成 ready，仍属于重试间隔结束。
        return 'retry_backoff' if cause == 'delayed' else 'retry_ready'
    return cause


def group_side(db, message, identifier, now, sub_events=None, redrive_times=None):
    """构造一个消费组在该消息上的对照侧写。"""
    channel = message['channel']
    group = resolve_group(db, channel, identifier)
    if group is None:
        return {
            'query': identifier, 'group': identifier, 'group_name': identifier,
            'subscription_id': None, 'exists': False,
            'status': 'unknown_group', 'state': None, 'classification': 'unknown_group',
            'label': STATUS_LABELS['unknown_group'],
            'not_dispatched': False, 'waiting': False, 'late': False,
            'redriven': False, 'new_identity': False,
            'markers': {'not_dispatched': False, 'waiting': False,
                        'redrive_new_identity': False, 'terminal_failure': False},
            'absence': {'code': 'group_unknown', 'label': ABSENCE_LABELS['group_unknown']},
            'absence_reason': 'group_unknown', 'reason': 'group_unknown',
            'deliveries': [], 'attempts': [], 'path': [], 'new_identities': [], 'rows': [],
        }

    sub_events = sub_events if sub_events is not None else _subscription_events(db, channel)
    pub_event = _publication_event(db, message['id'])
    sub_event = sub_events.get(group['name'])

    if pub_event is not None and sub_event is not None:
        existed_at_publication = sub_event['event_id'] < pub_event['id']
    else:
        # 旧日志缺失时退回时间戳；受控时钟可能同刻，故事件 id 才是主判据。
        existed_at_publication = group['created'] <= message['published']

    rows = db.execute(
        """SELECT d.* FROM deliveries d WHERE d.message_id=? AND d.subscription_id=?
           ORDER BY d.id""", (message['id'], group['id'])).fetchall()
    deliveries = []
    for row in rows:
        item = dict(row)
        item['attempts'] = attempts.for_delivery(db, item['id'])
        child = db.execute('SELECT id FROM deliveries WHERE parent_id=?', (item['id'],)).fetchone()
        item['redriven_as'] = child['id'] if child else None
        deliveries.append(item)

    base = {
        'query': identifier, 'group': group['name'], 'group_name': group['name'],
        'subscription_id': group['id'], 'tags': group['tags'], 'paused': group['paused'],
        'created_at': group['created'], 'exists': True,
        'existed_at_publication': existed_at_publication,
        'matched_at_publication': bool(deliveries) and deliveries[0]['parent_id'] is None,
        'dispatched_at_publication': any(d['parent_id'] is None for d in deliveries),
    }

    if not deliveries:
        if existed_at_publication is False:
            absence_code = 'group_created_after_publication'
        else:
            absence_code = 'tags_did_not_match'
        classification = f'not_dispatched_{absence_code}'
        absence = {'code': absence_code,
                   'reason': absence_code,
                   'late_group': absence_code == 'group_created_after_publication',
                   'tag_mismatch': absence_code == 'tags_did_not_match',
                   'label': ABSENCE_LABELS[absence_code]}
        return base | {
            'status': 'not_dispatched', 'state': None,
            'classification': classification,
            'absence': absence,
            'absence_reason': absence_code,
            'reason': absence_code,
            'late': absence['late_group'],
            'not_dispatched': True, 'waiting': False,
            'redriven': False, 'new_identity': False,
            'label': STATUS_LABELS['not_dispatched'],
            'markers': {'not_dispatched': True, 'waiting': False,
                        'redrive_new_identity': False, 'terminal_failure': False},
            'current': None, 'original': None, 'tip': None,
            'deliveries': [], 'attempts': [], 'path': [], 'new_identities': [],
            'attempt_count': 0, 'delivery_count': 0,
        }

    cause_map = {}
    for explained in eligibility.explain(db, group, now):
        cause_map[explained['id']] = explained

    view = []
    for item in deliveries:
        cause_row = cause_map.get(item['id'])
        view.append({
            'delivery_id': item['id'], 'policy_version': item['policy'],
            'state': item['state'], 'ready_at': item['ready_at'],
            'attempt_count': item['attempt_count'], 'terminal_at': item['terminal_at'],
            'parent_id': item['parent_id'], 'redriven_as': item['redriven_as'],
            'identity': 'redrive_successor' if item['parent_id'] else 'original',
            'new_identity': item['parent_id'] is not None,
            'cause': cause_row['cause'] if cause_row else None,
            'expires': cause_row['expires'] if cause_row else None,
            'attempts': item['attempts'],
        })

    original = next((v for v in view if v['parent_id'] is None), view[0])
    tip = view[-1]
    new_identities = [{'delivery_id': v['delivery_id'], 'parent_id': v['parent_id']}
                      for v in view if v['new_identity']]
    dead_identities = [v['delivery_id'] for v in view if v['state'] == 'dead']

    status = tip['state']
    waiting = status == 'waiting'
    waiting_kind = _waiting_kind(tip, cause_map.get(tip['delivery_id'])) if waiting else None
    retrying = waiting and any(a['outcome'] in ('nacked', 'expired') for a in tip['attempts'])
    leased_expired = status == 'leased' and tip.get('expires') is not None and tip['expires'] <= now

    if status == 'dead':
        classification = 'dead_redriven' if tip['redriven_as'] else 'dead'
    elif tip['new_identity']:
        classification = f'redrive_successor_{status}'
    elif waiting:
        classification = 'waiting_retry' if retrying else f'waiting_{waiting_kind}'
    else:
        classification = status

    markers = {
        'not_dispatched': False,
        'waiting': waiting,
        'retrying': retrying,
        'redrive_new_identity': bool(new_identities),
        'current_is_new_identity': tip['new_identity'],
        'terminal_failure': bool(dead_identities),
        'leased_expired': leased_expired,
    }

    current = None
    if waiting:
        current = {'delivery_id': tip['delivery_id'], 'state': 'waiting',
                   'kind': waiting_kind, 'cause': waiting_kind,
                   'label': WAIT_LABELS.get(waiting_kind, '等待中'),
                   'ready_at': tip['ready_at'],
                   'retry_in_seconds': max(0, tip['ready_at'] - now)}
    elif status == 'leased':
        current = {'delivery_id': tip['delivery_id'], 'state': 'leased',
                   'kind': 'expired' if leased_expired else 'leased',
                   'label': WAIT_LABELS['expired' if leased_expired else 'leased'],
                   'expires_at': tip.get('expires')}
    else:
        current = {'delivery_id': tip['delivery_id'], 'state': status,
                   'terminal_at': tip['terminal_at']}

    return base | {
        'status': status, 'state': status, 'classification': classification,
        'label': STATUS_LABELS.get(status, status),
        'waiting_kind': waiting_kind, 'retrying': retrying,
        'not_dispatched': False,
        'waiting': waiting,
        'late': False,
        'redriven': bool(new_identities),
        'new_identity': tip['new_identity'],
        'successor_id': new_identities[-1]['delivery_id'] if new_identities else None,
        'source_id': tip['parent_id'] if tip['new_identity'] else None,
        'terminal': status == 'dead' or bool(dead_identities),
        'terminal_failure': bool(dead_identities),
        'markers': markers, 'current': current,
        'original': original, 'tip': tip,
        'deliveries': view,
        'attempts': [attempt for item in view for attempt in item['attempts']],
        'path': _attempt_path(deliveries, redrive_times or _redrive_events(db, message['id']),
                              message['published']),
        'new_identities': new_identities,
        'dead_identities': dead_identities,
        'attempt_count': sum(len(v['attempts']) for v in view),
        'delivery_count': len(view),
    }


def snapshot(db, message_id, first, second, now):
    """生成两个组的只读对照快照（未签名）。"""
    first, second = require_pair(first, second)
    message = publication.get(db, message_id)
    sub_events = _subscription_events(db, message['channel'])
    redrive_times = _redrive_events(db, message_id)
    left = group_side(db, message, first, now, sub_events, redrive_times)
    right = group_side(db, message, second, now, sub_events, redrive_times)
    doc = {
        'format': 'quaybell-compare/1',
        'message_id': message_id,
        'message': {key: message[key] for key in
                    ('id', 'channel', 'producer', 'dedup_key', 'lane', 'policy',
                     'published', 'available', 'tags') if key in message} | {
                        'tags': message['envelope'].get('tags', {})},
        'channel': message['channel'],
        'observed_at': now,
        'pair': [first, second],
        'groups': [left['group_name'], right['group_name']],
        'left': left,
        'right': right,
        'sides': [left, right],
        'by_group': {left['group_name']: left, right['group_name']: right},
        'hash_algorithm': 'sha256',
        'canonical': 'quaybell-canonical-json/1',
    }
    doc['headline'] = {
        left['group_name']: {'status': left['status'],
                             'classification': left['classification'],
                             'label': left['label']},
        right['group_name']: {'status': right['status'],
                              'classification': right['classification'],
                              'label': right['label']},
    }
    doc['comparison'] = {'pair': doc['pair'], 'groups': doc['groups'],
                         'headline': doc['headline']}
    return doc
