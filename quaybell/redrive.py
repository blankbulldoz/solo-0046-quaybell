from . import delivery, eligibility, journal, policy, subscriptions
from .errors import conflict

LEGACY = {'recorded': False, 'reason': '早于此版本的重投没有留下提交时判断'}


def _source(db, key):
    item = delivery.get(db, key)
    if item['state'] != 'dead':
        raise conflict('投递尚未进入死信状态')
    return item


def _group(db, anchor):
    return subscriptions.get(db, anchor['channel'], anchor['group_name'])


def _remembered(db, anchor, source_id):
    '''读取提交时冻结的判断；旧格式事件没有冻结字段时如实标记。'''
    found = journal.recorded_redrive(db, anchor['message_id'], source_id)
    if found is None:
        return None
    fields = found['details']
    judgement = fields.get('judgement')
    if judgement is None:
        return {'submitted_at': None, 'judgement': dict(LEGACY, at=found['at'])}
    return {'submitted_at': fields.get('submitted_at'), 'judgement': judgement}


def _receipt(db, anchor, source_id, child_id, version, repeated, remembered, now):
    '''重投回执：judgement 是提交时冻结的先行债务，queue 是本次读取时的活队列位置。'''
    group = _group(db, anchor)
    frozen = remembered or {'judgement': LEGACY}
    judgement = frozen['judgement']
    preceding = judgement.get('preceding_ids', [])
    return {
        'source_id': source_id,
        'delivery_id': child_id,
        'policy': version,
        'repeated': repeated,
        'submitted_at': frozen.get('submitted_at'),
        'judgement': judgement,
        'queue': eligibility.progress(db, group, anchor['lane'], child_id, now, preceding),
    }


def detail(db, anchor, source_id, child_id, now):
    '''死信列表或轨迹里的重投视图；同样把原判断与当前队列位置分开。'''
    child = delivery.get(db, child_id)
    return _receipt(db, anchor, source_id, child_id, child['policy'], True,
                    _remembered(db, anchor, source_id), now)


def redrive(db, key, now):
    prior = _source(db, key)
    existing = db.execute('SELECT id,policy FROM deliveries WHERE parent_id=?', (key,)).fetchone()
    if existing:
        return _receipt(db, prior, key, existing['id'], existing['policy'], True,
                        _remembered(db, prior, key), now)
    active = policy.read(db, prior['channel'])
    new = delivery.add(db, prior['message_id'], prior['subscription_id'], prior['lane'],
                       active['version'], now, key)
    judgement = eligibility.snapshot(db, _group(db, prior), prior['lane'], new, now)
    stored = {'source_id': key, 'delivery_id': new, 'policy': active['version'],
              'submitted_at': now, 'judgement': judgement}
    journal.append(db, prior['channel'], now, 'redriven', stored, prior['message_id'])
    return _receipt(db, prior, key, new, active['version'], False, stored, now)
