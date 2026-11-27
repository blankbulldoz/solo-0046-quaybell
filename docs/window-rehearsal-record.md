# 两段切换窗口演练记录（自动生成）

- 演练锚定：2026-11-30（+08:00），受控时钟，单机回环 HTTP，`python3 -I docs/window_rehearsal.py` 可重放
- 策略：`{"lease_seconds": 90, "max_attempts": 1, "retention_seconds": 7200, "retry_seconds": 20}`
- 在途租约延长检查：{"old_policy": 1, "old_lease_seconds": 90.0, "new_policy": 2, "new_lease_seconds": 300.0}
- 回滚检查：{"paused_claim_items": 0, "ack_while_paused_state": "acked", "resumed_by": "edge-old", "resumed_delivery": 2}
- 有界回收检查：{"first_deadline": 1795740995.0, "second_deadline": 1795741000.0, "sweep1": [1], "sweep1_effective_at": 1795741030.0, "sweep2": [2], "counts_after_two_single_limited_sweeps": {}}
- 旧端最后一次取件：窗口前 30 s；租约统一在窗口后 60 s 到期。
- 说明：`board.messages` 仅保留最近 30 条，消息总量以发布计数为准；死信计数取 `/dead`，上限 50。

## 对照表

| 窗口 | 方式 | 窗口前发布 | 已结清 | 在途租约 | 暂停中老端确认 | 到期转死信 | 关窗时等待 | 窗口内新增 | 新端排空 | 批次 | 关窗死信 | 兄弟组残留 | 判定 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 09:00 | freeze | 54 | 0 | 18 | 0 | 18 | 36 | 6 | 37 | 3 | 18 | 12 阻挡 / 18 暂停 | HOLD (freeze strands every in-flight lease) |
| 09:00 | paced | 54 | 0 | 18 | 18 | 0 | 36 | 6 | 37 | 3 | 0 | 12 阻挡 / 18 暂停 | GO (paused drain, zero dead) |
| 09:40 | freeze | 80 | 40 | 20 | 0 | 20 | 20 | 18 | 21 | 2 | 20 | 29 阻挡 / 20 暂停 | HOLD (freeze strands every in-flight lease) |
| 09:40 | paced | 80 | 40 | 20 | 17 | 3 | 20 | 18 | 21 | 2 | 3 | 29 阻挡 / 20 暂停 | GO with handover list (3 dead) |

## 09:00 · freeze

- 暂停中取件：{'paused': True, 'items': 0}；暂停中发布仍分发：{'consumer': 82}
- 过期凭据确认：409；死信样例：{'delivery_id': 77, 'lane': 'lane-18', 'reason': '租约超时'}
- 证据摘要：5370cc1185b2e2e8af5d7f0bf70d19112199a8f7eeb943854143fed8f33185a5；凭据不出现在导出：True
- 关窗快照 observed_at=1795741800.0；死信 18 条 （[77, 73, 68, 64, 59, 55, 50, 46, 41, 37, 32, 28, 23, 19, 14, 10, 5, 1]）
- 同一时刻保留对照：兄弟组仍有 30 条未完成投递，当时保留候选 31 条；排空 30 条后解冻 30 条源消息，候选变为 61/61
- 判定：{"in_flight_at_cut": 18, "freeze_cost_dead_letters": 18, "paced_cost_dead_letters": 18, "paced_old_acks_settled": 0, "paced_old_acks_deferred_past_deadline": 0, "keys_waiting_at_cut": 36, "new_fleet_batches": 3, "new_fleet_drain_seconds_assumed": 15, "arrivals_in_window": 6, "residual_blocked_in_sibling": 12, "residual_paused_in_sibling": 18, "retention_pinned_by_sibling": 31, "sibling_waiting_at_close": 30, "source_messages_pinned_by_sibling": 30, "verdict": "HOLD (freeze strands every in-flight lease)", "variant": "freeze", "window": "early"}

| 偏移(s) | 动作 | consumer 计数 | drift-keeper 计数 | 死信 |
| --- | --- | --- | --- | --- |
| -120 | 发布 54 条（18 个顺序键 × 3） | {"blocked": 36, "ready": 18} | {"blocked": 9, "paused": 18} | 0 |
| -30 | 老端最后一次领取 18 条队首 | {"blocked": 36, "leased": 18} | {"blocked": 9, "paused": 18} | 0 |
| +0 | 暂停 consumer，新端不领取 | {"blocked": 36, "leased": 18} | {"blocked": 9, "paused": 18} | 0 |
| +0 | 暂停中发布 1 条（仍生成 consumer 分发） | {"blocked": 37, "leased": 18} | {"blocked": 9, "paused": 18} | 0 |
| +60 | 租约原截止时刻 | {"blocked": 37, "expired": 18} | {"blocked": 9, "paused": 18} | 0 |
| +60 | 全局回收，到期租约转死信 | {"blocked": 19, "paused": 18} | {"blocked": 9, "paused": 18} | 18 |
| +65 | 老端迟到确认 18 个：0 接受 / 18 拒绝 | {"blocked": 19, "paused": 18} | {"blocked": 9, "paused": 18} | 18 |
| +68 | 恢复 consumer，新端开始排空 | {"blocked": 19, "ready": 18} | {"blocked": 9, "paused": 18} | 18 |
| +83 | 新端 3 批排空 37 条 | {} | {"blocked": 9, "paused": 18} | 18 |
| +150 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 12, "paused": 18} | 18 |
| +150 | 新端当场确认新增 6 条 | {} | {"blocked": 12, "paused": 18} | 18 |
| +600 | 关窗 | {} | {"blocked": 12, "paused": 18} | 18 |
| +7800 | 复核：drift-keeper 排空 30 条后解冻保留 | {} | {} | 18 |

## 09:00 · paced

- 暂停中取件：{'paused': True, 'items': 0}；暂停中发布仍分发：{'consumer': 82}
- 过期凭据确认：未触发；死信样例：无
- 证据摘要：无；凭据不出现在导出：n/a
- 关窗快照 observed_at=1795741800.0；死信 0 条 （[]）
- 同一时刻保留对照：兄弟组仍有 30 条未完成投递，当时保留候选 31 条；排空 30 条后解冻 30 条源消息，候选变为 61/61
- 判定：{"in_flight_at_cut": 18, "freeze_cost_dead_letters": 18, "paced_cost_dead_letters": 0, "paced_old_acks_settled": 18, "paced_old_acks_deferred_past_deadline": 0, "keys_waiting_at_cut": 36, "new_fleet_batches": 3, "new_fleet_drain_seconds_assumed": 15, "arrivals_in_window": 6, "residual_blocked_in_sibling": 12, "residual_paused_in_sibling": 18, "retention_pinned_by_sibling": 31, "sibling_waiting_at_close": 30, "source_messages_pinned_by_sibling": 30, "verdict": "GO (paused drain, zero dead)", "variant": "paced", "window": "early"}

| 偏移(s) | 动作 | consumer 计数 | drift-keeper 计数 | 死信 |
| --- | --- | --- | --- | --- |
| -120 | 发布 54 条（18 个顺序键 × 3） | {"blocked": 36, "ready": 18} | {"blocked": 9, "paused": 18} | 0 |
| -30 | 老端最后一次领取 18 条队首 | {"blocked": 36, "leased": 18} | {"blocked": 9, "paused": 18} | 0 |
| +0 | 暂停 consumer，新端不领取 | {"blocked": 36, "leased": 18} | {"blocked": 9, "paused": 18} | 0 |
| +0 | 暂停中发布 1 条（仍生成 consumer 分发） | {"blocked": 37, "leased": 18} | {"blocked": 9, "paused": 18} | 0 |
| +5 | 暂停期间老端确认 3 条（累计成功 3） | {"blocked": 34, "leased": 15, "paused": 3} | {"blocked": 9, "paused": 18} | 0 |
| +15 | 暂停期间老端确认 3 条（累计成功 6） | {"blocked": 31, "leased": 12, "paused": 6} | {"blocked": 9, "paused": 18} | 0 |
| +25 | 暂停期间老端确认 3 条（累计成功 9） | {"blocked": 28, "leased": 9, "paused": 9} | {"blocked": 9, "paused": 18} | 0 |
| +35 | 暂停期间老端确认 3 条（累计成功 12） | {"blocked": 25, "leased": 6, "paused": 12} | {"blocked": 9, "paused": 18} | 0 |
| +45 | 暂停期间老端确认 3 条（累计成功 15） | {"blocked": 22, "leased": 3, "paused": 15} | {"blocked": 9, "paused": 18} | 0 |
| +55 | 暂停期间老端确认 3 条（累计成功 18） | {"blocked": 19, "paused": 18} | {"blocked": 9, "paused": 18} | 0 |
| +60 | 租约原截止时刻 | {"blocked": 19, "paused": 18} | {"blocked": 9, "paused": 18} | 0 |
| +60 | 全局回收，到期租约转死信 | {"blocked": 19, "paused": 18} | {"blocked": 9, "paused": 18} | 0 |
| +68 | 恢复 consumer，新端开始排空 | {"blocked": 19, "ready": 18} | {"blocked": 9, "paused": 18} | 0 |
| +83 | 新端 3 批排空 37 条 | {} | {"blocked": 9, "paused": 18} | 0 |
| +150 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 12, "paused": 18} | 0 |
| +150 | 新端当场确认新增 6 条 | {} | {"blocked": 12, "paused": 18} | 0 |
| +600 | 关窗 | {} | {"blocked": 12, "paused": 18} | 0 |
| +7800 | 复核：drift-keeper 排空 30 条后解冻保留 | {} | {} | 0 |

## 09:40 · freeze

- 暂停中取件：{'paused': True, 'items': 0}；暂停中发布仍分发：{'consumer': 121}
- 过期凭据确认：409；死信样例：{'delivery_id': 118, 'lane': 'lane-20', 'reason': '租约超时'}
- 证据摘要：22f3fa87de3f60949215345266849aa516f9dfd9429f8938477ecb7a52b246b3；凭据不出现在导出：True
- 关窗快照 observed_at=1795744200.0；死信 20 条 （[118, 112, 106, 100, 94, 88, 82, 76, 70, 64, 58, 52, 46, 40, 34, 28, 22, 16, 10, 4]）
- 同一时刻保留对照：兄弟组仍有 49 条未完成投递，当时保留候选 50 条；排空 49 条后解冻 49 条源消息，候选变为 99/99
- 判定：{"in_flight_at_cut": 20, "freeze_cost_dead_letters": 20, "paced_cost_dead_letters": 20, "paced_old_acks_settled": 0, "paced_old_acks_deferred_past_deadline": 0, "keys_waiting_at_cut": 20, "new_fleet_batches": 2, "new_fleet_drain_seconds_assumed": 10, "arrivals_in_window": 18, "residual_blocked_in_sibling": 29, "residual_paused_in_sibling": 20, "retention_pinned_by_sibling": 50, "sibling_waiting_at_close": 49, "source_messages_pinned_by_sibling": 49, "verdict": "HOLD (freeze strands every in-flight lease)", "variant": "freeze", "window": "heavy"}

| 偏移(s) | 动作 | consumer 计数 | drift-keeper 计数 | 死信 |
| --- | --- | --- | --- | --- |
| -120 | 发布 80 条（20 个顺序键 × 4） | {"blocked": 60, "ready": 20} | {"blocked": 20, "paused": 20} | 0 |
| -95 | 老端结清第 1 代 20 条 | {"blocked": 40, "ready": 20} | {"blocked": 20, "paused": 20} | 0 |
| -85 | 老端结清第 2 代 20 条 | {"blocked": 20, "ready": 20} | {"blocked": 20, "paused": 20} | 0 |
| -30 | 老端最后一次领取 20 条队首 | {"blocked": 20, "leased": 20} | {"blocked": 20, "paused": 20} | 0 |
| +0 | 暂停 consumer，新端不领取 | {"blocked": 20, "leased": 20} | {"blocked": 20, "paused": 20} | 0 |
| +0 | 暂停中发布 1 条（仍生成 consumer 分发） | {"blocked": 21, "leased": 20} | {"blocked": 20, "paused": 20} | 0 |
| +60 | 租约原截止时刻 | {"blocked": 21, "expired": 20} | {"blocked": 20, "paused": 20} | 0 |
| +60 | 全局回收，到期租约转死信 | {"blocked": 1, "paused": 20} | {"blocked": 20, "paused": 20} | 20 |
| +65 | 老端迟到确认 20 个：0 接受 / 20 拒绝 | {"blocked": 1, "paused": 20} | {"blocked": 20, "paused": 20} | 20 |
| +68 | 恢复 consumer，新端开始排空 | {"blocked": 1, "ready": 20} | {"blocked": 20, "paused": 20} | 20 |
| +78 | 新端 2 批排空 21 条 | {} | {"blocked": 20, "paused": 20} | 20 |
| +120 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 23, "paused": 20} | 20 |
| +120 | 新端当场确认新增 6 条 | {} | {"blocked": 23, "paused": 20} | 20 |
| +240 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 26, "paused": 20} | 20 |
| +240 | 新端当场确认新增 6 条 | {} | {"blocked": 26, "paused": 20} | 20 |
| +360 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 29, "paused": 20} | 20 |
| +360 | 新端当场确认新增 6 条 | {} | {"blocked": 29, "paused": 20} | 20 |
| +600 | 关窗 | {} | {"blocked": 29, "paused": 20} | 20 |
| +7800 | 复核：drift-keeper 排空 49 条后解冻保留 | {} | {} | 20 |

## 09:40 · paced

- 暂停中取件：{'paused': True, 'items': 0}；暂停中发布仍分发：{'consumer': 121}
- 过期凭据确认：409；死信样例：{'delivery_id': 118, 'lane': 'lane-20', 'reason': '租约超时'}
- 证据摘要：dd906d2383d48932a1ab0a69ba3f8a76b0521aa8dde883627490783b4e0e0cb0；凭据不出现在导出：True
- 关窗快照 observed_at=1795744200.0；死信 3 条 （[118, 112, 106]）
- 同一时刻保留对照：兄弟组仍有 49 条未完成投递，当时保留候选 50 条；排空 49 条后解冻 49 条源消息，候选变为 99/99
- 判定：{"in_flight_at_cut": 20, "freeze_cost_dead_letters": 20, "paced_cost_dead_letters": 3, "paced_old_acks_settled": 17, "paced_old_acks_deferred_past_deadline": 3, "keys_waiting_at_cut": 20, "new_fleet_batches": 2, "new_fleet_drain_seconds_assumed": 10, "arrivals_in_window": 18, "residual_blocked_in_sibling": 29, "residual_paused_in_sibling": 20, "retention_pinned_by_sibling": 50, "sibling_waiting_at_close": 49, "source_messages_pinned_by_sibling": 49, "verdict": "GO with handover list (3 dead)", "variant": "paced", "window": "heavy"}

| 偏移(s) | 动作 | consumer 计数 | drift-keeper 计数 | 死信 |
| --- | --- | --- | --- | --- |
| -120 | 发布 80 条（20 个顺序键 × 4） | {"blocked": 60, "ready": 20} | {"blocked": 20, "paused": 20} | 0 |
| -95 | 老端结清第 1 代 20 条 | {"blocked": 40, "ready": 20} | {"blocked": 20, "paused": 20} | 0 |
| -85 | 老端结清第 2 代 20 条 | {"blocked": 20, "ready": 20} | {"blocked": 20, "paused": 20} | 0 |
| -30 | 老端最后一次领取 20 条队首 | {"blocked": 20, "leased": 20} | {"blocked": 20, "paused": 20} | 0 |
| +0 | 暂停 consumer，新端不领取 | {"blocked": 20, "leased": 20} | {"blocked": 20, "paused": 20} | 0 |
| +0 | 暂停中发布 1 条（仍生成 consumer 分发） | {"blocked": 21, "leased": 20} | {"blocked": 20, "paused": 20} | 0 |
| +20 | 暂停期间老端确认 17 条（累计成功 17） | {"blocked": 4, "leased": 3, "paused": 17} | {"blocked": 20, "paused": 20} | 0 |
| +60 | 租约原截止时刻 | {"blocked": 4, "expired": 3, "paused": 17} | {"blocked": 20, "paused": 20} | 0 |
| +60 | 全局回收，到期租约转死信 | {"blocked": 1, "paused": 20} | {"blocked": 20, "paused": 20} | 3 |
| +70 | 老端迟到确认 3 个：0 接受 / 3 拒绝 | {"blocked": 1, "paused": 20} | {"blocked": 20, "paused": 20} | 3 |
| +72 | 恢复 consumer，新端开始排空 | {"blocked": 1, "ready": 20} | {"blocked": 20, "paused": 20} | 3 |
| +82 | 新端 2 批排空 21 条 | {} | {"blocked": 20, "paused": 20} | 3 |
| +120 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 23, "paused": 20} | 3 |
| +120 | 新端当场确认新增 6 条 | {} | {"blocked": 23, "paused": 20} | 3 |
| +240 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 26, "paused": 20} | 3 |
| +240 | 新端当场确认新增 6 条 | {} | {"blocked": 26, "paused": 20} | 3 |
| +360 | 窗口内新增 6 条 | {"ready": 6} | {"blocked": 29, "paused": 20} | 3 |
| +360 | 新端当场确认新增 6 条 | {} | {"blocked": 29, "paused": 20} | 3 |
| +600 | 关窗 | {} | {"blocked": 29, "paused": 20} | 3 |
| +7800 | 复核：drift-keeper 排空 49 条后解冻保留 | {} | {} | 3 |
