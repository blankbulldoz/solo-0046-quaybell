"""提交前的策略影响对照：只读，不改变任何投递或版本。"""
from hashlib import sha256

from . import codec, inputs, policy
from .errors import conflict

POLICY_KEYS = tuple(policy.LIMITS)
SAMPLE = 20


def policy_view(record):
    return {key: record[key] for key in POLICY_KEYS}


def policy_text(values):
    return (f"租约 {values['lease_seconds']} 秒 · 退避 {values['retry_seconds']} 秒 · "
            f"最多 {values['max_attempts']} 次 · 保留 {values['retention_seconds']} 秒")


def state_token(db, channel, candidate=None):
    """覆盖对照所依据的当前状态和（若有）拟修订值；提交时在同一写事务里重新计算。

    逐行汇总订阅、消息、投递与尝试的当前字段，时间流逝本身不改变标记。
    """
    digest = sha256()

    def add(kind, row):
        digest.update(kind.encode('ascii'))
        digest.update(codec.pack(row).encode('ascii'))
        digest.update(b'\n')

    if candidate is not None:
        add('candidate', [policy_view(candidate)])
    current = policy.read(db, channel)
    add('policy', [current['version'], *(current[key] for key in POLICY_KEYS)])
    for row in db.execute('SELECT id,name,tags,paused FROM subscriptions WHERE channel=? ORDER BY id',
                          (channel,)):
        add('subscription', list(row))
    for row in db.execute('''SELECT id,producer,dedup_key,lane,policy,published,available
                             FROM messages WHERE channel=? ORDER BY id''', (channel,)):
        add('message', list(row))
    for row in db.execute('''SELECT d.id,d.message_id,d.subscription_id,d.lane,d.policy,d.state,
                                    d.ready_at,d.attempt_count,d.terminal_at,d.parent_id
                             FROM deliveries d JOIN messages m ON m.id=d.message_id
                             WHERE m.channel=? ORDER BY d.id''', (channel,)):
        add('delivery', list(row))
    for row in db.execute('''SELECT a.id,a.delivery_id,a.number,a.outcome,a.expires,a.finished
                             FROM attempts a
                             JOIN deliveries d ON d.id=a.delivery_id
                             JOIN messages m ON m.id=d.message_id
                             WHERE m.channel=? ORDER BY a.id''', (channel,)):
        add('attempt', list(row))
    return digest.hexdigest()


def _sampled(items):
    return items[:SAMPLE], max(0, len(items) - SAMPLE)


def _versions(rows):
    return sorted({row['policy'] for row in rows})


def _version_text(versions):
    return '、'.join(f'v{version}' for version in versions) if versions else '无'


def _leases(db, channel, now, candidate):
    rows = [dict(row) for row in db.execute('''
        SELECT d.id AS delivery_id, d.message_id, d.lane, d.policy, d.attempt_count,
               a.number AS attempt, a.expires, s.name AS group_name
        FROM deliveries d
        JOIN messages m ON m.id=d.message_id
        JOIN subscriptions s ON s.id=d.subscription_id
        JOIN attempts a ON a.delivery_id=d.id AND a.outcome='open'
        WHERE m.channel=? AND d.state='leased'
        ORDER BY a.expires, d.id''', (channel,))]
    for item in rows:
        item['expired'] = item['expires'] <= now
        item['remaining_seconds'] = max(0, int(item['expires'] - now))
    shown, omitted = _sampled(rows)
    versions = _versions(rows)
    expired = sum(1 for item in rows if item['expired'])
    return {
        'key': 'in_hand_leases',
        'title': '在手租约',
        'certainty': 'settled',
        'applies': False,
        'trigger': None,
        'count': len(rows),
        'expired': expired,
        'summary': '租约在领取时按投递绑定的版本发放。本次修订不会改写已发放的到期时刻；'
                   '到期回收后再次领取、退避和尝试上限也继续按该投递绑定的版本计算。',
        'current': {'policy_versions': versions, 'expired': expired, 'items': shown},
        'proposed': {'applies': False},
        'current_text': (f"{len(rows)} 个在手租约（{expired} 个已到期待回收），绑定 "
                         f"{_version_text(versions)}；到期时刻已经确定。" if rows else '当前没有在手租约。'),
        'proposed_text': ('不追溯：已发放租约保持原到期时刻，再次领取仍用绑定版本。'
                          if rows else '提交后新发放的租约才使用拟修订值。'),
        'items': shown,
        'omitted': omitted,
    }


def _waiting(db, channel, now, candidate):
    rows = [dict(row) for row in db.execute('''
        SELECT d.id AS delivery_id, d.message_id, d.lane, d.policy, d.attempt_count,
               d.ready_at, s.name AS group_name
        FROM deliveries d
        JOIN messages m ON m.id=d.message_id
        JOIN subscriptions s ON s.id=d.subscription_id
        WHERE m.channel=? AND d.state='waiting' AND d.attempt_count>0
        ORDER BY d.ready_at, d.id''', (channel,))]
    rules = {}
    for item in rows:
        if item['policy'] not in rules:
            rules[item['policy']] = policy.read(db, channel, item['policy'])
        rule = rules[item['policy']]
        item['next_lease_seconds'] = rule['lease_seconds']
        item['next_backoff_seconds'] = policy.delay(rule, item['attempt_count'])
        item['attempts_left'] = max(0, rule['max_attempts'] - item['attempt_count'])
        item['phase'] = 'ready' if item['ready_at'] <= now else 'backoff'
    fresh = db.execute('''SELECT COUNT(*) FROM deliveries d JOIN messages m ON m.id=d.message_id
                          WHERE m.channel=? AND d.state='waiting' AND d.attempt_count=0''',
                       (channel,)).fetchone()[0]
    shown, omitted = _sampled(rows)
    versions = _versions(rows)
    ready = sum(1 for item in rows if item['phase'] == 'ready')
    backoff = len(rows) - ready
    return {
        'key': 'waiting_retries',
        'title': '等待重试',
        'certainty': 'settled',
        'applies': False,
        'trigger': None,
        'count': len(rows),
        'fresh_waiting': fresh,
        'summary': '这些投递在首次分发时就绑定了策略版本。下一次领取的租约、退避和尝试上限'
                   '都按绑定版本计算；本次修订只影响提交后新建立的分发。',
        'current': {'policy_versions': versions, 'ready': ready, 'backoff': backoff, 'items': shown},
        'proposed': {'applies': False},
        'current_text': (f"{len(rows)} 个等待重试（{backoff} 个仍在退避，{ready} 个已到可领取时间），"
                         f"绑定 {_version_text(versions)}；下一次租约与退避已按原版本确定。"
                         if rows else '当前没有等待重试。'),
        'proposed_text': (f"不追溯：提交后新绑定 v{candidate['version']} 的投递才使用 "
                          f"{policy_text(candidate)}。"
                          if rows else '提交后新绑定版本的投递才使用拟修订值。'),
        'fresh_note': (f"另有 {fresh} 个尚未开始重试的等待投递（含延迟未到就绪时间），同样已绑定原版本。"
                       if fresh else ''),
        'items': shown,
        'omitted': omitted,
    }


def _next_publish(db, channel, now, current, candidate):
    return {
        'key': 'next_publish',
        'title': '下一条新发布',
        'certainty': 'pending',
        'applies': True,
        'trigger': 'publish',
        'count': None,
        'summary': '策略版本在新消息建立分发时绑定。提交前发布的消息仍绑定当前版本；'
                   '只有提交之后真正发布并产生分发的消息才绑定拟修订版本。',
        'current': {'policy': policy_view(current)},
        'proposed': {'policy': policy_view(candidate)},
        'current_text': f"提交前发布：绑定 v{current['version']}，{policy_text(current)}。",
        'proposed_text': f"提交后发布：绑定 v{candidate['version']}，{policy_text(candidate)}。",
        'items': [],
        'omitted': 0,
    }


def _redrive(db, channel, now, candidate):
    rows = [dict(row) for row in db.execute('''
        SELECT d.id AS delivery_id, d.message_id, d.lane, d.policy, d.terminal_at,
               s.name AS group_name,
               (SELECT c.id FROM deliveries c WHERE c.parent_id=d.id) AS redriven_as
        FROM deliveries d
        JOIN messages m ON m.id=d.message_id
        JOIN subscriptions s ON s.id=d.subscription_id
        WHERE m.channel=? AND d.state='dead'
        ORDER BY d.id''', (channel,))]
    available = [item for item in rows if item['redriven_as'] is None]
    shown, omitted = _sampled(available)
    versions = _versions(rows)
    return {
        'key': 'dead_redrive',
        'title': '死信重投',
        'certainty': 'pending',
        'applies': True,
        'trigger': 'redrive',
        'count': len(available),
        'already_redriven': len(rows) - len(available),
        'bound_versions': versions,
        'summary': '重投在调用时绑定当时生效的策略。已追加的后继保持其创建时的版本；'
                   '原消息的保留年龄仍按发布时版本，不会因重投或本次修订而改变。',
        'current': {'policy_versions': versions, 'items': shown},
        'proposed': {'policy': policy_view(candidate)},
        'current_text': (f"{len(available)} 个死信尚无后继，历史绑定 {_version_text(versions)}；"
                         f"若在提交前重投，后继绑定当前版本。"
                         if available else '当前没有可重投的死信。'),
        'proposed_text': (f"提交后重投：后继绑定 v{candidate['version']}，租约 "
                          f"{candidate['lease_seconds']} 秒 · 退避 {candidate['retry_seconds']} 秒 · "
                          f"最多 {candidate['max_attempts']} 次；原消息保留仍按发布时版本。"),
        'items': shown,
        'omitted': omitted,
    }


def comparison(db, channel, now, current, candidate):
    effects = [_leases(db, channel, now, candidate),
               _waiting(db, channel, now, candidate),
               _next_publish(db, channel, now, current, candidate),
               _redrive(db, channel, now, candidate)]
    settled = sum(item['count'] or 0 for item in effects if item['certainty'] == 'settled')
    return {
        'channel': channel,
        'observed_at': now,
        'baseline': {'version': current['version'], 'policy': policy_view(current),
                     'token': state_token(db, channel, candidate),
                     'state': state_token(db, channel)},
        'proposed': candidate,
        'binding_note': '策略在发布或重投建立投递时绑定；修订只影响之后新建的绑定。'
                        '已有消息的保留年龄按发布时版本计算，也不会追溯。',
        'certainty_note': 'settled 表示影响已经确定且不会被本次修订改写；'
                          'pending 表示要等后续发布或重投才发生，且以那时的状态为准。',
        'settled_count': settled,
        'effects': effects,
    }


def preview(db, channel, raw, now):
    inputs.require(raw, (*policy.LIMITS, 'expected'))
    expected = inputs.integer(raw['expected'], 1, 2 ** 63 - 2, '当前版号')
    current = policy.read(db, channel)
    if current['version'] != expected:
        raise conflict('投递策略已更新，请重新载入后再预查')
    candidate = policy.checked({key: raw[key] for key in policy.LIMITS}) | {'version': expected + 1}
    return comparison(db, channel, now, current, candidate)
