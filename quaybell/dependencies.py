from . import delivery, inputs, journal
from .errors import conflict, invalid


def _waiting_delivery(db, key):
    item = delivery.get(db, key)
    if item['state'] != 'waiting':
        raise conflict('只有等待中的投递才能声明前置投递')
    return item


def _ids(raw):
    inputs.require(raw, ('requires',))
    values = raw['requires']
    if not isinstance(values, list) or not 1 <= len(values) <= 16:
        raise invalid('前置投递须为 1～16 个投递号组成的数组')
    result = []
    for value in values:
        if type(value) is not int or not 1 <= value <= 2**63 - 2:
            raise invalid('前置投递号须为正整数')
        if value not in result:
            result.append(value)
    return result


def _depends_on(db, prerequisite_id, target_id):
    """前置的（传递）依赖链里是否出现目标本身；出现即构成循环。"""
    seen, frontier = set(), {prerequisite_id}
    while frontier:
        current = frontier.pop()
        for row in db.execute('SELECT requires_id FROM delivery_prerequisites WHERE delivery_id=?',
                              (current,)).fetchall():
            if row['requires_id'] == target_id:
                return True
            if row['requires_id'] not in seen:
                seen.add(row['requires_id'])
                frontier.add(row['requires_id'])
    return False


def declare(db, key, raw, now):
    target = _waiting_delivery(db, key)
    requires = _ids(raw)
    if key in requires:
        raise invalid('投递不能把自身声明为前置')
    for other in requires:
        prerequisite = delivery.get(db, other)
        if prerequisite['subscription_id'] != target['subscription_id']:
            raise conflict(f'前置投递 #{other} 属于其他消费组，无法在本组声明依赖')
        if prerequisite['lane'] == target['lane'] and prerequisite['id'] > target['id']:
            raise conflict(
                f'同顺序键 {target["lane"]} 上投递 #{prerequisite["id"]} 排在 #{target["id"]} 之后，'
                '它会先被本投递挡住而永远无法确认，该依赖无法推进')
        if _depends_on(db, other, key):
            raise conflict(f'前置投递 #{other} 已依赖本投递，声明会形成循环依赖')
    existing = {row['requires_id'] for row in db.execute(
        'SELECT requires_id FROM delivery_prerequisites WHERE delivery_id=?', (key,))}
    added = [other for other in requires if other not in existing]
    db.executemany('INSERT INTO delivery_prerequisites(delivery_id,requires_id,declared) VALUES (?,?,?)',
                   [(key, other, now) for other in added])
    result = {'delivery_id': key, 'requires': for_delivery(db, key), 'added': added,
              'repeated': not bool(added)}
    journal.append(db, target['channel'], now, 'requirements_declared',
                   {'delivery_id': key, 'added': added, 'requires': result['requires']},
                   target['message_id'])
    return result


def for_delivery(db, key):
    return [row['requires_id'] for row in db.execute(
        'SELECT requires_id FROM delivery_prerequisites WHERE delivery_id=? ORDER BY requires_id', (key,))]


def gate_cache(db, group_id):
    """一次性读取本组全部前置关系及前置当前状态，供领取和积压解释共用。"""
    rows = db.execute('''SELECT p.delivery_id,p.requires_id,d.state,a.reason
                         FROM delivery_prerequisites p
                         JOIN deliveries d ON d.id=p.requires_id
                         LEFT JOIN attempts a ON a.delivery_id=d.id
                          AND a.id=(SELECT MAX(id) FROM attempts WHERE delivery_id=d.id)
                         WHERE d.subscription_id=?''', (group_id,)).fetchall()
    gates = {}
    for row in rows:
        detail = dict(delivery_id=row['requires_id'], state=row['state'], reason=row['reason'])
        gates.setdefault(row['delivery_id'], []).append(detail)
    for details in gates.values():
        details.sort(key=lambda detail: detail['delivery_id'])
    return gates


def gate_of(details):
    """返回 (cause, 未解除的前置明细)。全部确认则为 (None, [])。"""
    blocking = [detail for detail in details if detail['state'] != 'acked']
    if not blocking:
        return None, []
    if any(detail['state'] == 'dead' for detail in blocking):
        return 'gated_dead', blocking
    return 'gated', blocking
