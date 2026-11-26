"""同键队首的领取资格与看板归因。

调度领取（``claims`` -> :func:`ready`）和看板积压原因
（``overview`` -> :func:`explain`）是同一条顺序不变量的两个消费面，
本模块是这份职责的唯一归属：

* **值得共享的是"谁挡住谁"的定义**：同一消费组、同一顺序键（lane）上，
  存在 ID 更小且仍未处置（``waiting``/``leased``）的投递时，当前投递
  不能越过它。终态 ``acked``/``dead`` 不阻挡，所以回收或确认队首后后继
  自动推进。:data:`OPEN_STATES`、:func:`earlier_open_blocker` 和
  :func:`lane_heads` 是同一条规则的 SQL / Python 两种形态。

* **不应共享的是判定的执行形状与外围归因**。领取是写事务里的有界取键：
  调用方先做组内有界回收，:func:`ready` 再把阻挡谓词下推为命中
  ``delivery_lane`` 部分索引的相关子查询，并用 ``LIMIT`` 把返回键数限制在
  30 以内；不要为了复用 Python 队首而把组内全量未处置行物化进领取事务。
  看板是单个只读快照，本来就要枚举组内全部未处置投递来计数和取样，
  :func:`explain` 在 Python 侧标出队首并叠加租约、暂停和时间归因。

暂停和回收都不属于资格事实：暂停闸门与"暂停时不回收"在
``claims.claim``，只读看板通过 :func:`attribute` 叠加 ``paused`` 归因；
回收是 ``expiry``/``outcomes`` 的写职责，看板只按开放尝试的截止时间诊断
``expired``（越过截止但尚未回收），绝不代为回收。

下一次加排序策略时，只改 :data:`CLAIM_ORDER` 标出的领取排序接缝（以及
可能的有限键投影）；阻挡谓词、开放状态集合和看板归因优先级应保持不动，
两条路径才不会在排序演进中漂移。
"""

from .delivery import DELIVERY

# 仍会占据同键队首、阻挡后继的状态。两条路径共用这一份定义。
OPEN_STATES = ('waiting', 'leased')
# 由 OPEN_STATES 派生的内联 IN 列表；状态是内部常量而非用户输入。
OPEN_STATES_SQL = '(' + ', '.join(f"'{state}'" for state in OPEN_STATES) + ')'

# 领取排序接缝：当前公开合同固定按投递 ID 升序（见 PROJECT_SPEC）。
# 未来的排序策略只替换此表达式，不应放宽下面的队首阻挡谓词。
CLAIM_ORDER = 'd.id'


def earlier_open_blocker(alias='d'):
    """共享不变量的 SQL 形态：存在同组同键、ID 更小的未处置投递。"""
    return f'''NOT EXISTS (SELECT 1 FROM deliveries p
             WHERE p.subscription_id={alias}.subscription_id AND p.lane={alias}.lane AND p.id<{alias}.id
               AND p.state IN {OPEN_STATES_SQL})'''


def ready(db, group_id, now, limit):
    """返回最多 ``limit`` 个可领取的就绪键（按 :data:`CLAIM_ORDER` 排序）。

    只回答调度事实：未处置队首、``waiting``、``ready_at<=now`` 且无更早
    未处置投递。不读取/判断暂停（由 ``claims.claim`` 在更外层闸门处理），
    不回收租约、不写库；过期租约需调用方先经 ``expiry.reap`` 回收，仍以
    ``leased`` 形态占据队首的过期投递自然不会出现在结果中。
    """
    return [dict(row) for row in db.execute(DELIVERY + f'''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND {earlier_open_blocker('d')}
        ORDER BY {CLAIM_ORDER} LIMIT ?''', (group_id, now, limit))]


def lane_heads(rows):
    """共享不变量的 Python 形态：按 ID 升序的行中每键首行 ID 即队首。

    返回 ``{lane: head_delivery_id}``。仅供需要全量行的看板路径使用；
    领取热路径应继续走 :func:`ready` 的有界 SQL，不要调用本函数。
    """
    heads = {}
    for row in rows:
        heads.setdefault(row['lane'], row['id'])
    return heads


def attribute(item, head_id, paused, now):
    """按固定公开优先级给一条未处置投递归类当前主因。

    1. ``leased`` 行先看开放尝试截止：``expires<=now`` 为 ``expired``
       （已越截止但尚未回收），否则 ``leased``（租约有效）；
    2. 不是本键队首 => ``blocked``（即使组暂停，后继仍记阻挡）；
    3. 队首遇暂停 => ``paused``；
    4. 队首未到 ``ready_at`` => ``delayed``，否则 ``ready``。
    """
    if item['state'] == 'leased':
        return 'expired' if item['expires'] <= now else 'leased'
    if item['id'] != head_id:
        return 'blocked'
    if paused:
        return 'paused'
    return 'delayed' if item['ready_at'] > now else 'ready'


def explain(db, group, now):
    """枚举组内全部未处置投递并给出 ``cause``/``head_id`` 看板归因。

    只读：不回收过期租约。因此队首租约越过截止后、被领取前扫描或全局
    ``/sweep`` 真正回收之前，看板会持续将其记为 ``expired`` 且后继仍为
    ``blocked``；回收提交后它回到 ``waiting``，归因随之变为
    ``delayed``/``ready``，尝试用尽时转为 ``dead`` 并离开此结果，后继
    继任队首。
    """
    rows = [dict(row) for row in db.execute(
        f'''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires
            FROM deliveries d LEFT JOIN attempts a
              ON a.delivery_id=d.id AND a.outcome='open'
            WHERE d.subscription_id=? AND d.state IN {OPEN_STATES_SQL} ORDER BY d.id''',
        (group['id'],))]
    head_ids = lane_heads(rows)
    paused = bool(group['paused'])
    for item in rows:
        head_id = head_ids[item['lane']]
        cause = attribute(item, head_id, paused, now)
        yield item | {'cause': cause, 'head_id': head_id}
