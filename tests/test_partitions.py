"""分片分配与同键单一授权进程的回归测试。

同时覆盖纯函数与真实 HTTP 入口（临时 SQLite、受控时钟、本机随机端口）。
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from examples.session import GROUP, session  # noqa: E402
from quaybell.assignments import partition_for  # noqa: E402


def separated_pair(n2=2, n3=3):
    names = [f'case-key-{i:03d}' for i in range(500)]
    for hot in names:
        for idle in names:
            if hot != idle and partition_for(hot, n2) != partition_for(idle, n2) \
                    and partition_for(hot, n3) != partition_for(idle, n3):
                return hot, idle
    raise AssertionError('no separated pair')


class PartitionHashTests(unittest.TestCase):
    def test_stable_and_bounded(self):
        for lane in ('coil-north', 'device-7', '热键'):
            first = partition_for(lane, 8)
            self.assertEqual(first, partition_for(lane, 8))
            self.assertIn(first, range(8))

    def test_count_changes_can_remap_but_same_count_stable(self):
        self.assertEqual(partition_for('abc', 2), partition_for('abc', 2))


class AssignmentHttpTests(unittest.TestCase):
    def setUp(self):
        self._ctx = session()
        self.client = self._ctx.__enter__()
        self.addCleanup(self._ctx.__exit__, None, None, None)
        self.client.channel(dict(lease_seconds=30, retry_seconds=1,
                                 max_attempts=8, retention_seconds=3600))
        self.client.subscribe()

    def configure_two(self):
        return self.client.request(GROUP + '/partitions',
                                   {'partitions': 2, 'workers': ['alpha', 'beta']})

    def test_unconfigured_group_keeps_open_claim(self):
        self.client.publish('p1', 'any-lane')
        items = self.client.claim(1, worker='random-process')
        self.assertEqual(len(items), 1)
        self.assertIsNone(items[0]['assignment_version'])

    def test_only_owner_receives_key_and_view_reports_version(self):
        view = self.configure_two()
        self.assertEqual(view['version'], 1)
        hot, idle = separated_pair()
        hp = partition_for(hot, 2)
        owner = view['partitions'][hp]['owner']
        other = 'beta' if owner == 'alpha' else 'alpha'
        self.client.publish('h', hot)
        self.client.publish('i', idle)
        got_owner = self.client.claim(3, worker=owner)
        self.assertEqual([i['lane'] for i in got_owner], [hot])
        got_other = self.client.claim(3, worker=other)
        self.assertTrue(all(i['lane'] != hot for i in got_other))
        self.assertEqual(self.client.claim(1, worker='nobody'), [])
        state = self.client.request(GROUP + '/assignments')
        row = next(k for k in state['keys'] if k['lane'] == hot)
        self.assertEqual(row['owner'], owner)
        self.assertEqual(row['partition'], hp)
        self.assertEqual(row['version'], 1)

    def test_explicit_partition_not_owned_is_forbidden(self):
        view = self.configure_two()
        self.client.publish('h', 'some-lane')
        alpha_parts = [p['partition'] for p in view['partitions'] if p['owner'] == 'alpha']
        forbidden = next(p for p in range(2) if p not in alpha_parts)
        self.client.request(GROUP + '/claim',
                            {'worker': 'alpha', 'partition': forbidden}, 403)

    def test_idle_key_moves_immediately_and_hot_debt_orders_first(self):
        hot, idle = separated_pair()
        first = self.configure_two()
        hot_owner = first['partitions'][partition_for(hot, 2)]['owner']
        idle_owner = first['partitions'][partition_for(idle, 2)]['owner']
        self.client.publish('debt', hot)
        self.client.publish('tail', hot)
        self.client.publish('idle', idle)
        debt = self.client.claim(1, worker=hot_owner)[0]
        self.client.settle(debt, 'nack')
        self.client.tick(1)
        old_lease = self.client.claim(1, worker=hot_owner)[0]
        self.assertEqual(old_lease['lease']['assignment_version'], 1)
        idle_item = self.client.claim(1, worker=idle_owner)[0]
        self.client.settle(idle_item)

        hp3, ip3 = partition_for(hot, 3), partition_for(idle, 3)
        rest = next(p for p in range(3) if p not in (hp3, ip3))
        new_hot, new_idle = 'beta', 'gamma'
        # 让热键改属 beta（若 beta 恰为旧属主则换成另一个名字），空闲键改属 gamma
        if new_hot == hot_owner:
            new_hot = 'alpha' if hot_owner == 'beta' else 'beta'
        plan = {str(hp3): new_hot, str(ip3): new_idle, str(rest): hot_owner}
        second = self.client.request(GROUP + '/rebalance',
                                    {'partitions': 3, 'assignment': plan, 'expected': 1})
        self.assertEqual(second['version'], 2)
        self.assertEqual(len(second['fenced_leases']), 1)
        # 旧版凭据被围栏
        self.client.settle(old_lease, 'ack', expected=409)
        # 空闲键立即由新进程领取，不需要推进时钟
        self.client.publish('idle-2', idle)
        moved = self.client.claim(1, worker=new_idle)
        self.assertEqual([i['lane'] for i in moved], [idle])
        # 热键新属主先拿到旧债务，且批内没有后继
        batch = self.client.claim(3, worker=new_hot)
        self.assertEqual([i['delivery_id'] for i in batch], [debt['delivery_id']])
        self.assertEqual(batch[0]['lease']['assignment_version'], 2)
        self.client.settle(batch[0])
        tail = self.client.claim(1, worker=new_hot)[0]
        self.assertNotEqual(tail['message_id'], debt['message_id'])
        self.assertEqual(tail['lane'], hot)

    def test_expected_version_conflict(self):
        self.configure_two()
        self.client.request(GROUP + '/partitions',
                            {'partitions': 3, 'workers': ['alpha', 'beta', 'gamma'],
                             'expected': 99}, 409)

    def test_idempotent_same_plan_does_not_bump(self):
        self.configure_two()
        again = self.client.request(GROUP + '/partitions',
                                    {'partitions': 2, 'workers': ['alpha', 'beta']})
        self.assertTrue(again.get('repeated'))
        self.assertEqual(again['version'], 1)

    def test_authorize_endpoint(self):
        view = self.configure_two()
        lane = 'auth-lane'
        partition = partition_for(lane, 2)
        owner = view['partitions'][partition]['owner']
        result = self.client.request(GROUP + '/authorize',
                                    {'worker': owner, 'lanes': [lane, 'missing-lane']})
        decisions = {d['lane']: d['authorized'] for d in result['decisions']}
        self.assertTrue(decisions[lane])
        self.assertIn('missing-lane', decisions)

    def test_honor_policy_keeps_old_token_usable(self):
        view = self.configure_two()
        lane = 'honor-lane'
        p = partition_for(lane, 2)
        owner = view['partitions'][p]['owner']
        other = 'beta' if owner == 'alpha' else 'alpha'
        self.client.publish('k', lane)
        taken = self.client.claim(1, worker=owner)[0]
        # 把该分片改属另一进程，但选择不围栏：旧凭据在租约期内仍可处置
        self.client.request(GROUP + '/partitions',
                            {'partitions': 2, 'assignment': {str(p): other,
                                                             str(1 - p): owner},
                             'lease_policy': 'honor', 'expected': 1})
        result = self.client.settle(taken, 'ack')
        self.assertEqual(result['state'], 'acked')

    def test_members_listed_after_claim(self):
        self.configure_two()
        self.client.publish('m', 'member-lane')
        self.client.claim(1, worker='alpha')
        members = self.client.request(GROUP + '/members')
        names = {m['worker'] for m in members['members']}
        self.assertIn('alpha', names)


if __name__ == '__main__':
    unittest.main()
