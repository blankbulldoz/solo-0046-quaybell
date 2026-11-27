"""Machine-checked classification used by the lease-deadline acceptance report.

``INVARIANT`` rows must hold on every run and every schedule.  ``SCHEDULING``
rows may legitimately differ between runs; the tests that own them assert only
the legal envelope (a bound or a set of allowed values), never a fixed value.
"""
from collections import namedtuple

Row = namedtuple('Row', 'name kind asserted_by note')

CONTRACT = (
    Row('attempt_numbering', 'invariant', 'test_attempt_number_is_monotonic_and_unique',
        '一次投递的尝试号严格 1,2,3…，任一时刻至多一条 open'),
    Row('token_identity', 'invariant', 'test_token_binds_to_exactly_one_attempt',
        '凭据只匹配签发它的投递与尝试；旧凭据在新租约下不能结算'),
    Row('deadline_comparison', 'invariant', 'test_deadline_is_inclusive',
        'expires <= now 即视为超时；now == expires 与 now > expires 同类'),
    Row('single_terminal_outcome', 'invariant', 'test_no_dual_success_or_timeout',
        '同一尝试不会既是 acked 又是 expired；投递终态与尝试终态一致'),
    Row('ack_after_deadline_rejected', 'invariant', 'test_ack_at_deadline_is_rejected',
        '截止时刻及以后的旧凭据 ACK 一律 409，且不新增尝试记录'),
    Row('successor_blocked_by_head', 'invariant', 'test_successor_never_overtakes_head',
        '同顺序键后继在队首 waiting/leased 时永不可领取'),
    Row('retry_origin', 'invariant', 'test_expiry_backoff_starts_at_deadline',
        '超时重试的 ready_at = 原 expires + 退避，而非回收时刻'),
    Row('repeat_receipt', 'invariant', 'test_before_deadline_ack_is_accepted_and_successor_waits',
        '同一凭据重复 ACK 返回同一条已记录回执（repeated=true）'),
    Row('journal_shape', 'invariant', 'test_journal_records_each_transition_once',
        '每次尝试的 leased 与终态事件各至多一次，不因竞争重复'),
    Row('ack_winner_count', 'invariant', 'test_concurrent_acks_have_one_winner',
        '并发 ACK 中 repeated=false 的首次受理恰好一次'),
    Row('clock_crossing_exclusive', 'invariant', 'test_clock_crossing_deadline_mid_ack_and_sweep',
        '两个请求跨截止读取时钟时，attempt 1 的 acked 与 expired 仍互斥'),
    Row('winner_identity', 'scheduling', 'test_clock_crossing_deadline_mid_ack_and_sweep',
        '跨截止竞争里 ACK 与 sweep 谁先读到时钟，决定 ACK 受理还是 409'),
    Row('busy_contention', 'scheduling', 'test_concurrent_acks_have_one_winner',
        '可能出现的 503 busy 次数随调度变化，重试后结果不变'),
    Row('reap_actor', 'scheduling', 'test_concurrent_sweep_and_claim_report_recovery_once',
        '由 sweep 还是 claim 的预扫描报告本次回收不固定'),
    Row('expiry_finished_time', 'scheduling', 'test_late_sweep_finished_time_is_within_deadline_and_trigger',
        '回收晚于截止时 finished 落在 [expires, now] 之间，等于触发回收的时刻'),
    Row('observed_attempt_count', 'scheduling', 'test_concurrent_read_sees_one_consistent_attempt',
        '竞争进行中读到 open 还是终态由读快照的时机决定'),
)


def by_kind(kind):
    return tuple(row for row in CONTRACT if row.kind == kind)


if __name__ == '__main__':
    for row in CONTRACT:
        print(f'{row.kind:10} {row.name:28} {row.asserted_by}')
