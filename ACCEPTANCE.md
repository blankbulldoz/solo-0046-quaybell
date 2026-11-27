# Quaybell 两端口滚动替换 · 防误连上线验收记录

- 生成时间：2026-11-27T00:51:33+00:00
- 总判定：**全部通过，可放行**
- 端口：A=`18091`，B=`18092`，误连旧副本 C=`18093`（仅阴性对照）
- 共同库：`/workspace/.rollout-lab/common/quaybell.sqlite3`
- 故意保留的旧库：`/workspace/.rollout-lab/stale-copy/quaybell.sqlite3`
- 验收方式：真实子进程 + 真实 8 秒租约自然过期 + 真实 HTTP 跨端口并发；未调用 pause/ack/nack/redrive/retention，未删除任何业务消息。

## 1. 检查项总表

| 结果 | 检查 | 说明 |
| --- | --- | --- |
| PASS | `init_cluster_rc0` | rc=0 |
| PASS | `two_nodes_boot_guarded` | A=baseline-r053 B=baseline-r053 |
| PASS | `boot_guard_pass_line` | 两个初始进程的日志均有 QB_GUARD_PASS |
| PASS | `create_lease_channel` | status=201 |
| PASS | `create_gears_group` | status=201 |
| PASS | `create_proof_channel` | status=201 |
| PASS | `create_flow_channel` | status=201 |
| PASS | `create_flow_group` | status=201 |
| PASS | `publish_m1` | status=201 id=1 |
| PASS | `publish_m2` | status=201 id=2 |
| PASS | `race1_no_http_errors` | errors=0 busy=0 |
| PASS | `race1_exactly_one_lease_for_head` | winners=1 total_items=1 |
| PASS | `race1_successor_never_leased` | d2_items=0 |
| PASS | `race1_single_open_attempt_in_db` | partial unique index + BEGIN IMMEDIATE 后仅一行 open 尝试 |
| PASS | `race1_token_digest_matches_db` | attempt=#1 via port 18091 |
| PASS | `decoy_copy_rc0` | sha256=d2fd1fecdea72fd2… |
| PASS | `restored_copy_rc0` |  |
| PASS | `freeze_logical_hashes_equal` | 复制瞬间两份库核心状态逻辑哈希一致 |
| PASS | `freeze_lease_identical_in_copy` | attempt#1 race-one-4 expires=1795740701.5172253 |
| PASS | `copy_taken_while_lease_alive` | now=1795740693.6 expires=1795740701.5 |
| PASS | `decoy_http_trace_same_initially` | A、B 共库端与误连 C 起初经 HTTP 给出完全相同的消息指纹和租约摘要 |
| PASS | `naive_channel_name_check_passes_on_decoy` | 两端频道名一致: ['rollout-flow', 'rollout-lease', 'rollout-proof']（证明只查频道名无法识别误连） |
| PASS | `fence_a_created` | {   "ok": true,   "http_status": 201,   "spec_path": "/workspace/.rollout-lab/specs/fence-a.json",   "fence_id": 1,   "message_id": 3,   "journal_watermark": 9 } |
| PASS | `launch_rejects_wrong_path_exit3` | rc=3 code=DB_PATH_MISMATCH |
| PASS | `launch_rejects_stale_replica_exit4` | rc=4 code=STALE_REPLICA |
| PASS | `verify_common_rc0` | rc=0 |
| PASS | `verify_stale_nonzero_with_failing_checks` | rc=3 failures=['path_pin', 'fence_row', 'fence_message', 'journal_watermark'] |
| PASS | `node_a_stopped_one_at_a_time` | A0 已停止，B0 仍在服务 |
| PASS | `partner_b_takes_work_while_a_down` | 发布=201 领取条数=1（不确认，租约自然保留） |
| PASS | `a1_guarded_boot_pass` | QB_GUARD_PASS {"cluster": "quaybell-rolling-acceptance", "candidate": "/workspace/.rollout-lab/common/quaybell.sqlite3", "journal_max": 11, "fence": "fence=#1"} |
| PASS | `a1_release_header_new` | X-Quaybell-Release=candidate-r053 |
| PASS | `replaced_a1_sees_fact_committed_by_b0` | F-A #3 在 A1=200 B0=200 且指纹一致 |
| PASS | `a1_write_visible_to_unrestarted_b0` | F-A2 #5 A1 提交，A1=200/B0 旧进程=200 指纹一致 |
| PASS | `fence_b_created` | rc=0 |
| PASS | `partner_a1_takes_work_while_b_down` | 发布=201 领取条数=1 |
| PASS | `replaced_b1_sees_fact_committed_by_a1` | F-B #6 B1 读取=200 |
| PASS | `b1_write_visible_to_a1` | F-B2 #8 B1 提交，A1 读取=200 |
| PASS | `both_ports_now_candidate_release` | A1=candidate-r053 B1=candidate-r053 |
| PASS | `original_lease_naturally_expired_still_open` | 超过截止 0.8s 且回收前 outcome 仍为 open（未做任何处置） |
| PASS | `peer_reads_same_open_past_deadline` | 对端 HTTP 只读也看到同一行 open 尝试 |
| PASS | `peer_sweep_reclaims_attempt1` | [{"attempt": 1, "delivery_id": 1, "effective_at": 1795740701.5172253, "outcome": "expired", "reason": "租约超时", "state": "waiting"}] |
| PASS | `race2_exactly_one_replacement_lease` | winners=1 attempt=#2 |
| PASS | `race2_new_token_differs` | 第二次尝试使用全新凭据，旧凭据随尝试关闭失效 |
| PASS | `race2_successor_still_blocked` | 同键后继 D2 在新队首未处置前仍不可领取 |
| PASS | `race2_single_open_attempt_in_db` | 共库任何时刻只有一行 open 尝试（one_open_attempt 部分唯一索引） |
| PASS | `shared_db_state_advanced` | 共库尝试序列 = #1 expired -> #2 open，journal 连续推进 |
| PASS | `a1_b1_views_identical` | 两个滚动节点对推进后状态的 HTTP 视图完全一致 |
| PASS | `decoy_frozen_pre_reclaim_snapshot` | 旧副本仍只有 #1/open（截止时间同样已过），没有 expired 记录与 #2 |
| PASS | `post_fork_fact_404_on_decoy` | 栅栏事实 F-A #3 在误连端为 404，共库两端为 200 |
| PASS | `controlled_split_brain_tokens_differ` | 旧副本自行回收后发出与共库不同的 #2 凭据——这正是护栏阻止的重复租约脑裂 |
| PASS | `stale_copy_retained_on_disk` | 旧库保留在 /workspace/.rollout-lab/stale-copy/quaybell.sqlite3 |

## 2. 可复制的上线命令（本次实际执行）

```sh
# 一次性：初始化共同库绑定
python3 ops/qb_rollout.py init-cluster --db /workspace/.rollout-lab/common/quaybell.sqlite3 --cluster quaybell-rolling-acceptance

# 替换 A 前：经未重启的 B 提交栅栏事实
python3 ops/qb_rollout.py fence --db /workspace/.rollout-lab/common/quaybell.sqlite3 --via http://127.0.0.1:18092 --channel rollout-proof --out /workspace/.rollout-lab/specs/fence-a.json

# 带护栏启动替换端 A（路径钉 + 集群 + 新鲜事实）
QUAYBELL_RELEASE=candidate-r053 python3 -m quaybell --port 18091 --db /workspace/.rollout-lab/common/quaybell.sqlite3 --common-db /workspace/.rollout-lab/common/quaybell.sqlite3 --expect-cluster quaybell-rolling-acceptance --require-fence /workspace/.rollout-lab/specs/fence-a.json

# 替换 B 前：经已换新的 A 再提交一道栅栏，然后同样带护栏启动 B
python3 ops/qb_rollout.py fence --db /workspace/.rollout-lab/common/quaybell.sqlite3 --via http://127.0.0.1:18091 --channel rollout-proof --out /workspace/.rollout-lab/specs/fence-b.json
QUAYBELL_RELEASE=candidate-r053 python3 -m quaybell --port 18092 --db /workspace/.rollout-lab/common/quaybell.sqlite3 --common-db /workspace/.rollout-lab/common/quaybell.sqlite3 --expect-cluster quaybell-rolling-acceptance --require-fence /workspace/.rollout-lab/specs/fence-b.json
```

护栏拒绝码：`2` 库不存在（不隐式建空库）、`3` 路径不是权威共同库、`4` 路径/绑定正确但缺少栅栏新鲜事实＝旧副本、`5` 非受管库或集群不符、`6` 栅栏文件损坏。

## 3. “能拒绝误连”的实录

### 3.1 wrong_path

```sh
/usr/bin/python3 -m quaybell --port 18099 --db /workspace/.rollout-lab/stale-copy/quaybell.sqlite3 --common-db /workspace/.rollout-lab/common/quaybell.sqlite3 --expect-cluster quaybell-rolling-acceptance --require-fence /workspace/.rollout-lab/specs/fence-a.json
```
退出码：**3**；护栏码：`DB_PATH_MISMATCH`

```json
{
  "code": "DB_PATH_MISMATCH",
  "exit_code": 3,
  "failures": [
    {
      "check": "path_pin",
      "code": "DB_PATH_MISMATCH",
      "message": "候选库 /workspace/.rollout-lab/stale-copy/quaybell.sqlite3 不是权威共同库 /workspace/.rollout-lab/common/quaybell.sqlite3；这通常意味着 --db 指向了复制旧库"
    },
    {
      "check": "fence_row",
      "code": "STALE_REPLICA",
      "message": "候选库看不到栅栏行 #1；该栅栏产生于复制 fork 之后，说明候选库是落后的旧副本，而不是状态已推进的共库"
    },
    {
      "check": "fence_message",
      "code": "STALE_REPLICA",
      "message": "候选库看不到未重启端刚提交的事实：消息 #3 不存在"
    },
    {
      "check": "journal_watermark",
      "code": "STALE_REPLICA",
      "message": "候选库 journal 水位 8 低于栅栏水位 9；共库已推进而候选库停在旧快照"
    }
  ]
}
```
```text
拒绝启动 Quaybell：[path_pin] 候选库 /workspace/.rollout-lab/stale-copy/quaybell.sqlite3 不是权威共同库 /workspace/.rollout-lab/common/quaybell.sqlite3；这通常意味着 --db 指向了复制旧库
拒绝启动 Quaybell：[fence_row] 候选库看不到栅栏行 #1；该栅栏产生于复制 fork 之后，说明候选库是落后的旧副本，而不是状态已推进的共库
拒绝启动 Quaybell：[fence_message] 候选库看不到未重启端刚提交的事实：消息 #3 不存在
拒绝启动 Quaybell：[journal_watermark] 候选库 journal 水位 8 低于栅栏水位 9；共库已推进而候选库停在旧快照
```

### 3.2 stale_pinned_path

```sh
/usr/bin/python3 -m quaybell --port 18098 --db /workspace/.rollout-lab/restored-canonical/quaybell.sqlite3 --common-db /workspace/.rollout-lab/restored-canonical/quaybell.sqlite3 --expect-cluster quaybell-rolling-acceptance --require-fence /workspace/.rollout-lab/specs/fence-a.json
```
退出码：**4**；护栏码：`STALE_REPLICA`

```json
{
  "code": "STALE_REPLICA",
  "exit_code": 4,
  "failures": [
    {
      "check": "fence_row",
      "code": "STALE_REPLICA",
      "message": "候选库看不到栅栏行 #1；该栅栏产生于复制 fork 之后，说明候选库是落后的旧副本，而不是状态已推进的共库"
    },
    {
      "check": "fence_message",
      "code": "STALE_REPLICA",
      "message": "候选库看不到未重启端刚提交的事实：消息 #3 不存在"
    },
    {
      "check": "journal_watermark",
      "code": "STALE_REPLICA",
      "message": "候选库 journal 水位 8 低于栅栏水位 9；共库已推进而候选库停在旧快照"
    }
  ]
}
```
```text
拒绝启动 Quaybell：[fence_row] 候选库看不到栅栏行 #1；该栅栏产生于复制 fork 之后，说明候选库是落后的旧副本，而不是状态已推进的共库
拒绝启动 Quaybell：[fence_message] 候选库看不到未重启端刚提交的事实：消息 #3 不存在
拒绝启动 Quaybell：[journal_watermark] 候选库 journal 水位 8 低于栅栏水位 9；共库已推进而候选库停在旧快照
```

`ops/qb_rollout.py verify` 对旧副本的结论：

```json
{
  "ok": false,
  "code": "DB_PATH_MISMATCH",
  "checks": {
    "db_file": "pass",
    "path_pin": "fail",
    "binding": "pass",
    "cluster": "pass",
    "fence_row": "fail",
    "fence_message": "fail",
    "journal_watermark": "fail"
  },
  "observed": {
    "binding_cluster": "quaybell-rolling-acceptance",
    "canonical_realpath": "/workspace/.rollout-lab/common/quaybell.sqlite3",
    "journal_max": 8,
    "core_schema": true
  },
  "failures": [
    {
      "check": "path_pin",
      "code": "DB_PATH_MISMATCH",
      "message": "候选库 /workspace/.rollout-lab/stale-copy/quaybell.sqlite3 不是权威共同库 /workspace/.rollout-lab/common/quaybell.sqlite3；这通常意味着 --db 指向了复制旧库"
    },
    {
      "check": "fence_row",
      "code": "STALE_REPLICA",
      "message": "候选库看不到栅栏行 #1；该栅栏产生于复制 fork 之后，说明候选库是落后的旧副本，而不是状态已推进的共库"
    },
    {
      "check": "fence_message",
      "code": "STALE_REPLICA",
      "message": "候选库看不到未重启端刚提交的事实：消息 #3 不存在"
    },
    {
      "check": "journal_watermark",
      "code": "STALE_REPLICA",
      "message": "候选库 journal 水位 8 低于栅栏水位 9；共库已推进而候选库停在旧快照"
    }
  ]
}
```

## 4. 同键不重复发租约

- **首次跨两端口 8 并发抢领**：总发放条数 `1`，D1 赢家数 `1`，D2（同键后继）发放数 `0`，HTTP 错误 `0`，503 忙重试 `0`，各端口发放 `{'18091': 1, '18092': 0}`

  ```json
  {"port": 18091, "worker": "race-one-4", "delivery_id": 1, "attempt_number": 1, "expires": 1795740701.5172253, "token_prefix": "JxT-PGkjfa", "token_digest": "c9c00033c9bdf6c3…"}
  ```
- **回收后第二次 8 并发抢领**：总发放条数 `1`，D1 赢家数 `1`，D2（同键后继）发放数 `0`，HTTP 错误 `0`，503 忙重试 `0`，各端口发放 `{'18091': 1, '18092': 0}`

  ```json
  {"port": 18091, "worker": "race-two-4", "delivery_id": 1, "attempt_number": 2, "expires": 1795740710.2799418, "token_prefix": "SIHCeSZT7o", "token_digest": "c3963f33636b7216…"}
  ```

机制：claim 写事务以 `BEGIN IMMEDIATE` 串行化，提交后队首转为 `leased`，后继的 `NOT EXISTS (... 更早的 waiting/leased 同键投递)` 谓词不成立；SQLite 另有 `one_open_attempt` 部分唯一索引兜底，同一投递至多一行 `outcome='open'`。

## 5. 如何分清“共库已推进”与“启动到了旧副本”

| 核对点 | 两个滚动端口（共同库，状态已推进） | 误连的旧副本（fork 后冻结） |
| --- | --- | --- |
| M1 尝试序列 | `[(1, 'expired'), (2, 'open')]` | `[(1, 'open')]` |
| fork 后栅栏消息 F-A | 两端 `200`，指纹一致 | `404 not_found` |
| journal 水位/栅栏行 | 达到栅栏水位且有 published 事件 | 水位落后、无栅栏行（启动护栏 exit 4） |
| 若对旧副本也执行 sweep+claim（阴性实验） | 共库当前 open 凭据摘要唯一 | 旧副本发出 #`2`，与共库凭据不同＝脑裂：`True` |

**判别口径**：过期时刻本身不能作为依据——旧副本墙上的 `expires` 同样会过去，但在它自己的回收器运行前会一直停在 `#1/open`；真正的证据是 ① 启动栅栏（fork 后事实可见性 + journal 水位）；② 尝试行是否已出现 `expired` 收口与新编号/新摘要的尝试；③ 两端 HTTP 追溯视图是否逐字段一致。若一个“替换端”给不出栅栏事实或停在旧尝试序列，它不是看到了推进后的共库，而是连着旧副本。

自然过期与回收实录：

```json
{
  "holder_port": 18091,
  "sweep_via_port": 18092,
  "pre_sweep": {
    "now": 1795740702.2690613,
    "expires": 1795740701.5172253,
    "seconds_past_deadline": 0.75,
    "outcome": "open"
  },
  "sweep_recovered_d1": [
    {
      "attempt": 1,
      "delivery_id": 1,
      "effective_at": 1795740701.5172253,
      "outcome": "expired",
      "reason": "租约超时",
      "state": "waiting"
    }
  ]
}
```

## 6. 滚动替换与连续性

- 替换 `A`：伙伴端提交事实 `#3`，替换端启动护栏通过并读到该事实，发布代号 `candidate-r053`。
- 替换 `B`：伙伴端提交事实 `#6`，替换端启动护栏通过并读到该事实，发布代号 `candidate-r053`。
- `A_down` 期间另一端接活：发布 `201`，领取 `200`，发放 `1` 条。
- `B_down` 期间另一端接活：发布 `201`，领取 `200`，发放 `1` 条。

响应头 `X-Quaybell-Release` 用于区分应答进程代次；数据级放行证据是跨进程读到伙伴刚提交的事实。

## 7. 产物

```json
{
  "common_db_sha256_file": "1fc8686ba24188f23ed795f75b687a8cf4c1918b7af1059318d024e94cacc0f1",
  "stale_db_sha256_file": "712a7f60e45b6ba2b86287b69d07188aacfd0e1c83d88f1b0b0b35d072834022",
  "common_logical_hash_final": "8ab07ed754926e04fd1157ab5d831446878adaf099bcc04c00c0d9617b5b5622",
  "stale_logical_hash_final": "8dd079b49d94468ac836eb2144049ef4e8466c6d088768ee4110a53d3147b6e5",
  "stale_copy_retained_at": "/workspace/.rollout-lab/stale-copy/quaybell.sqlite3",
  "logs": {
    "A": "/workspace/.rollout-lab/logs/A1-new-p18091.log",
    "B": "/workspace/.rollout-lab/logs/B1-new-p18092.log",
    "C": "/workspace/.rollout-lab/logs/C-decoy-p18093.log"
  },
  "spec_a": "/workspace/.rollout-lab/specs/fence-a.json",
  "spec_b": "/workspace/.rollout-lab/specs/fence-b.json",
  "pristine_fork_copy": "/workspace/acceptance/evidence/pristine-fork-copy.sqlite3",
  "brain_split_copy": "/workspace/acceptance/evidence/stale-copy-brain-split.sqlite3"
}
```

复跑：`python3 ops/rolling_acceptance.py`（约 20 秒；自动建/清 `.rollout-lab/`，结束默认停进程并保留旧库、栅栏 JSON 与节点日志；加 `--keep` 保留运行进程）。
