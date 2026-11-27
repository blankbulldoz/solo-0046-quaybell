# 租约截止点的并发验收报告

本报告说明 `tests/test_lease_deadline_acceptance.py` 覆盖什么、断言什么，以及哪些
现象被明确允许随调度变化。分类表由 `tests/contract.py` 定义，测试运行时自检"表中
每一行都对应一个真实测试、且报告提到每一行"，因此文档与用例不会静默漂移。

## 如何本地重跑

无需第三方包（仅 Python 3.11+ 自带 `unittest`）：

```sh
python3 -m unittest discover -s tests -t . -v
# 只跑本验收：
python3 -m unittest tests.test_lease_deadline_acceptance -v
# 打印不变的契约分类表：
python3 -m tests.contract
```

每个用例各自通过真实 HTTP 监听器起一台中继、临时 SQLite 文件与受控时钟，
用例结束即释放端口与目录，彼此不共享状态。并发用例用 `threading.Barrier`
把多个真实请求同时释放，不使用 `sleep`。

## 三种边界输入

固定一条队首投递（attempt 1，截止点 `E`）与同顺序键后继，按受控时钟取：

| 输入 | 时钟 | 期望旧凭据结果 | 后继可见性 |
| --- | --- | --- | --- |
| 截止前 | `E-1`（也可 `E-0.5`） | ACK 受理，`state=acked` | 队首被清后才可领取 |
| 等于截止 | `E` | ACK 409 `conflict`（含等于即超时） | 仍被阻挡，直到尝试 2 被领取或处置 |
| 回收后新租约 | `>E` 且已 `sweep` 并签发尝试 2 | 旧凭据 ACK/NACK 均 409 | 仍被阻挡，队首为重试尝试 2 |

"回收后已生成新租约" 用例刻意先 `sweep`（回收 attempt 1，`effective_at=E+退避`），
再在 `E+3` 领取到 attempt 2，然后用 attempt 1 的凭据 ACK/NACK，验证不会误伤新租约。

## 每次都成立的观察（invariant）

这些行在任意调度、任意重复运行下都必须成立，测试对它们逐个断言：

| 名称 | 含义 |
| --- | --- |
| attempt_numbering | 尝试号严格 `1,2,3…`；任一时刻至多一条 `open` |
| token_identity | 凭据只匹配签发它的投递与尝试；旧凭据在新租约下不能结算 |
| deadline_comparison | `expires <= now` 即超时；`now == expires` 与之后同类 |
| single_terminal_outcome | 同一尝试不会既是 `acked` 又是 `expired`，投递终态与尝试终态一致 |
| ack_after_deadline_rejected | 截止时刻及以后的旧凭据 ACK 一律 409，且不新增尝试记录 |
| successor_blocked_by_head | 同键后继在队首 `waiting/leased` 时永不可领取 |
| retry_origin | 超时重试 `ready_at = 原 expires + 退避`，与回收时刻无关 |
| repeat_receipt | 同一凭据重复 ACK 返回同一条已记录回执（`repeated=true`） |
| journal_shape | 每次尝试的 `leased` 与终态事件各至多一次，竞争不产生重复 |
| ack_winner_count | 并发 ACK 中 `repeated=false` 的首次受理恰好一次 |
| clock_crossing_exclusive | 两个请求跨截止读取时钟时，attempt 1 的 acked 与 expired 仍互斥 |

关键判定一句话总结：**旧凭据的 ACK 与回收不会同时被确认为成功和超时**——要么
attempt 1 记录为 `acked`（且此后再无 `expired` 事件），要么记录为 `expired`
（且 ACK 返回 409、日志中无 `acked`）；`tests/contract.py` 的
`single_terminal_outcome` 与 `journal_shape` 两行分别从尝试记录和事件流两侧锁定这一点。

### 跨截止点的真实竞争

`test_clock_crossing_deadline_mid_ack_and_sweep` 把两个请求同时释放，并用
`Clock.arm(E-1, E)` 让"跨过截止点"发生在两次调用之间。服务端每请求在写事务内读一次
时钟，因此两种交错的观察都合法：

- ACK 读到 `E-1`、sweep 读到 `E` → ACK 受理，sweep 无可回收；
- sweep 读到 `E-1`、ACK 读到 `E` → ACK 返回 409，attempt 1 仍为 `open`。

（第三种"先回收再读时钟"的形态由 `test_late_sweep_then_ack_does_not_succeed`
确定性地覆盖。）三种形态下都不允许 attempt 1 同时出现 `acked` 与 `expired`，
也不允许同键后继越过未终态的队首。

## 允许因调度而变的观察（scheduling）

这些行只在"合法包络"内断言，**不**固定具体取值：

| 名称 | 允许的变化 | 测试如何界定 |
| --- | --- | --- |
| winner_identity | 跨截止竞争里 ACK 与 sweep 谁先读到时钟 | 只要求 attempt 1 的 `acked`/`expired` 互斥，且后继不抢先 |
| busy_contention | 出现 503 `busy` 的次数 | 工具自动重试；断言只要求最终结果正确 |
| reap_actor | 由 `/sweep` 还是 `claim` 的预扫描报告本次回收 | 只要求尝试 1 恰好一次 `expired`，并发报告至多一条 |
| expiry_finished_time | 回收晚于截止时 `finished` 的时刻 | 断言 `expires <= finished <= 本次触发时刻` |
| observed_attempt_count | 竞争期间读到 `open` 还是终态 | 读快照允许旧或新，但必须自洽（`open` 时 `finished` 为空） |

`claim` 每次先做组内预扫描回收，所以"回收由谁触发"天然不可控；`ThreadingHTTPServer`
的线程交错也会改变 `503 busy` 次数。这些波动被有意排除在断言之外。

## 不变量之外还锁定的事实

- 尝试记录只在同一条投递上递进，跨投递（另一顺序键）的凭据被拒。
- 死信（尝试用尽）后队首变为终态，同键后继才开始可领取；`redrive` 追加一个
  按当前策略绑定的新投递，`source_id` 指回死信。
- 同一脚本连跑两次，消息 ID、投递 ID、尝试行、事件序列、终态完全一致
  （`test_same_script_is_identical_across_runs`、`test_boundary_script_is_deterministic`）。
- 发布顺序置换不改变"最小投递 ID 为队首"的事实
  （`test_publish_order_permutation_keeps_min_id_first`）。

## 边界说明

- 时钟由测试注入，边界是精确整数秒；毫秒级"读后写"窗口不在本验收范围内。
- `<=` 比较意味着 `now == expires` 必然超时；若将来改为严格小于，`deadline_comparison`
  与 `deadline_is_inclusive` 会立即失败，属于有意的回归信号。
- 用例只使用 `examples.session` 暴露的公共 HTTP 入口，未直接操作 SQLite。
