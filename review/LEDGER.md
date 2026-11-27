# 告警清理后原键复用 · 交接账本

账本修订 r3，更新于 2026-11-27T18:22:06Z。

> 本账本用本机同构样本 review/evidence/run-001 标注每条手记能确认到哪一步。样本只证明机制上讲得通，不是生产事实；任何生产结论都要由值班人按引用核到生产侧的编号与时间。

## 证据台阶

| 台阶 | 含义 |
| --- | --- |
| S0 仅手记 | 只有叙述，没有本机材料，也没有可复现路径。 |
| S1 同构可复现 | 本机同构样本能跑出这一步；说明机制上讲得通，不指向生产。 |
| S2 材料可核对 | 至少一条本机保留材料按字段对上，且摘要校验通过。 |
| S3 双源交叉 | 两条相互独立的材料（如迟来回执与保留证据）在关键字段上一致。 |

## 声明

| 声明 | 内容 | 能确认到 | 判定 | 处置 | 会否改写新消息那一行 |
| --- | --- | --- | --- | --- | --- |
| C1 05:12 已补发 | new-message | S2 材料可核对 | 收窄 | 采用（收窄） | 不会 |
| C2 05:18 再次收到 | new-message | S2 材料可核对 | 收窄 | 采用（收窄） | 不会 |
| C3 05:21 已处理 | new-message | S2 材料可核对 | 与材料相反 | 不得改写 | 会 |
| C4 迟来的重投回执（父子分发号） | old-chain | S3 双源交叉 | 一致（仅旧链） | 采用（限旧链） | 不会 |
| C5 保留下来的新消息证据 | artifact | S2 材料可核对 | 一致 | 采用 | 不会 |
| C6 补充材料：时间接近但链路不同 | peer-chain | S2 材料可核对 | 字段一致，链路未合并 | 仅登记，不并账 | 不会 |
| C7 库内已无这三条消息 | artifact | S2 材料可核对 | 一致 | 采用 | 不会 |

### C1 · 05:12 已补发

同构样本中 05:12:00 生产方以原生产键 alert-7 重新发布，得到新消息 #2（repeated=false），并向 primary-desk 生成分发 3。

- 台阶：S2 材料可核对
- 判定：收窄；处置：采用（收窄）
- 限定：「补发」在本样本里是新的消息身份，不是幂等重放；不能用它证明生产侧是重放还是新发。

| 引用 | 字段 | 期望值 |
| --- | --- | --- |
| `review/evidence/run-001/facts.json` | `new_publish.message_id` | `2` |
| `review/evidence/run-001/facts.json` | `new_publish.repeated` | `false` |
| `review/evidence/run-001/facts.json` | `new_publish.fanout.primary-desk` | `3` |
| `review/evidence/run-001/evidence-new.json` | `trace.message.id` | `2` |
| `review/evidence/run-001/evidence-new.json` | `trace.message.published` | `1795756320.0` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.id` | `3` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.parent_id` | `null` |

待核：
- 生产方是否把「补发」理解为同一批次重放，需要生产侧书面确认；账本不替它猜。

### C2 · 05:18 再次收到

05:18:00 desk-a 领取新消息的分发 3，租约 300 秒至 05:23:00；05:22:00 导出的证据显示该尝试仍为 open，投递状态 leased。

- 台阶：S2 材料可核对
- 判定：收窄；处置：采用（收窄）
- 限定：「收到」只到「已领取、持有租约」；本账本不把它当作处理完成。

| 引用 | 字段 | 期望值 |
| --- | --- | --- |
| `review/evidence/run-001/facts.json` | `new_claim.delivery_id` | `3` |
| `review/evidence/run-001/facts.json` | `new_claim.worker` | `"desk-a"` |
| `review/evidence/run-001/facts.json` | `new_claim.expires` | `1795756980.0` |
| `review/evidence/run-001/evidence-new.json` | `observed_at` | `1795756920.0` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.state` | `"leased"` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.attempts.0.started` | `1795756680.0` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.attempts.0.outcome` | `"open"` |

### C3 · 05:21 已处理

与材料相反：新消息分发 3 在 05:21 前后没有确认记录；租约 05:23:00 到期，05:23:30 的全局回收把它记为 expired 并转入死信。样本里 05:21:00 的确认属于另一条链路的分发 4。

- 台阶：S2 材料可核对
- 判定：与材料相反；处置：不得改写
- 限定：这条手记只能停留在手记；不得把新消息那一行标成已处理。

| 引用 | 字段 | 期望值 |
| --- | --- | --- |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.attempts.0.outcome` | `"open"` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.attempts.0.finished` | `null` |
| `review/evidence/run-001/facts.json` | `sweep.recovered.0.delivery_id` | `3` |
| `review/evidence/run-001/facts.json` | `sweep.recovered.0.outcome` | `"expired"` |
| `review/evidence/run-001/facts.json` | `sweep.recovered.0.effective_at` | `1795757010.0` |
| `review/evidence/run-001/facts.json` | `mirror_ack.delivery_id` | `4` |
| `review/evidence/run-001/facts.json` | `mirror_ack.effective_at` | `1795756860.0` |

待核：
- 若生产侧确有 05:21 的处理动作，须给出消息号或分发号；只有「已处理」三字挂不到新消息上。

### C4 · 迟来的重投回执（父子分发号）

回执中的父子分发号与本机保留材料对上：旧消息 #1 的分发 1 被明确拒绝后，重投追加直接后继分发 2，分发 2 已在 05:00:06 确认。

- 台阶：S3 双源交叉
- 判定：一致（仅旧链）；处置：采用（限旧链）
- 限定：回执与保留证据两条独立材料在 1→2 与其后继已确认上一致；这只证明旧链有后继，不能把新消息那一行改成已处理。

| 引用 | 字段 | 期望值 |
| --- | --- | --- |
| `review/evidence/run-001/facts.json` | `original_redrive.source_id` | `1` |
| `review/evidence/run-001/facts.json` | `original_redrive.delivery_id` | `2` |
| `review/evidence/run-001/evidence-original.json` | `trace.deliveries.1.parent_id` | `1` |
| `review/evidence/run-001/evidence-original.json` | `trace.deliveries.1.state` | `"acked"` |
| `review/evidence/run-001/evidence-original.json` | `trace.message.id` | `1` |
| `review/supplements/receipt-1-2.json` | `parent_delivery` | `1` |
| `review/supplements/receipt-1-2.json` | `child_delivery` | `2` |

### C5 · 保留下来的新消息证据

05:22:00 导出的 quaybell-trace/1，摘要与记录内容自洽；其中 message #2、delivery #3、尝试 open。

- 台阶：S2 材料可核对
- 判定：一致；处置：采用
- 限定：证据冻结的是 05:22:00 的读取快照，不覆盖之后发生的事。

| 引用 | 字段 | 期望值 |
| --- | --- | --- |
| `review/evidence/run-001/evidence-new.json` | `format` | `"quaybell-trace/1"` |
| `review/evidence/run-001/evidence-new.json` | `sha256` | `"13e7cfd9feb554603eb254098280d3e80a1f615ab65fd7c89d68c4d8705645c8"` |
| `review/evidence/run-001/evidence-new.json` | `trace.message.id` | `2` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.id` | `3` |
| `review/evidence/run-001/evidence-new.json` | `trace.deliveries.0.attempts.0.outcome` | `"open"` |

### C6 · 补充材料：时间接近但链路不同

本机可核对：补充材料自带 identity 指向 alert-mirror 的消息 #3 / 分发 4，该分发在 05:21:00 被确认；它与告警本体 alert-desk 的消息 #2 / 分发 3 信封逐字段相同，但链路身份不同。

- 台阶：S2 材料可核对
- 判定：字段一致，链路未合并；处置：仅登记，不并账
- 限定：共 6 个身份字段（producer、key、lane、tags、payload、fingerprint）一致；但 channel=alert-mirror、message_id=3、delivery_id=4、parent_id=null，且发布于 05:12:05（新消息发布后 5 秒）。字段同构可以解释成镜像转发，也可能只是另一处按同一模板重发；证据本身分不开这两种解释。

| 关联层 | 是否成立 | 依据 |
| --- | --- | --- |
| 字段级 | 成立 | 信封与指纹逐字段相同 |
| 链路级 | 未成立 | 频道与消息身份不同，分发 4 无父节点 |
| 业务级 | 需人核对 | 缺少镜像转发规则或生产侧同批次说明 |

| 引用 | 字段 | 期望值 |
| --- | --- | --- |
| `review/evidence/run-001/evidence-mirror.json` | `observed_at` | `1795756890.0` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.message.channel` | `"alert-mirror"` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.message.id` | `3` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.message.published` | `1795756325.0` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.deliveries.0.id` | `4` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.deliveries.0.parent_id` | `null` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.deliveries.0.state` | `"acked"` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.deliveries.0.attempts.0.started` | `1795756685.0` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.deliveries.0.attempts.0.finished` | `1795756860.0` |
| `review/evidence/run-001/evidence-mirror.json` | `trace.deliveries.0.attempts.0.outcome` | `"acked"` |
| `review/evidence/run-001/facts.json` | `mirror_publish.fanout.primary-desk` | `4` |
| `review/evidence/run-001/facts.json` | `mirror_ack.effective_at` | `1795756860.0` |
| `review/evidence/run-001/evidence-new.json` | `trace.message.channel` | `"alert-desk"` |
| `review/evidence/run-001/evidence-new.json` | `trace.message.id` | `2` |
| `review/evidence/run-001/facts.json` | `new_publish.message_id` | `2` |
| `review/evidence/run-001/facts.json` | `mirror_publish.message_id` | `3` |

待核：
- 要把两条链路定为人核对后的关联，需要生产侧提供 alert-mirror 的转发规则，人核对后再决定是否在同一次告警下并账；账本不代替这一步。

### C7 · 库内已无这三条消息

05:24:00 清理移除消息 #2；05:27:00 直接读取消息 1、2、3 均为 404；两个频道的保留候选为空。此后只能依赖保留材料。

- 台阶：S2 材料可核对
- 判定：一致；处置：采用
- 限定：这不是「已处理」，只是「已不在库中」；清理与处置是两件事。

| 引用 | 字段 | 期望值 |
| --- | --- | --- |
| `review/evidence/run-001/facts.json` | `new_prune.removed.0` | `2` |
| `review/evidence/run-001/facts.json` | `final_reads.1` | `404` |
| `review/evidence/run-001/facts.json` | `final_reads.2` | `404` |
| `review/evidence/run-001/facts.json` | `final_reads.3` | `404` |
| `review/evidence/run-001/facts.json` | `final_retention.alert-desk.eligible` | `[]` |
| `review/evidence/run-001/facts.json` | `mirror_prune.removed.0` | `3` |

## 材料锚点

| 锚点 | 类型 | 文件 | 能证明 | 不能证明 |
| --- | --- | --- | --- | --- |
| receipt-1-2 | redrive-receipt | `review/supplements/receipt-1-2.json` | 旧链 1 → 2 存在直接后继，且 2 已确认。 | 不涉及新消息 #2 / 分发 3 的任何处置。 |
| supplement-mirror | peer-trace | `review/supplements/supplement-mirror.json` | 字段级同构：生产者、生产键、顺序键、标签、载荷、指纹全部一致，时间相差 5 秒。 | 不证明它与告警本体是同一次分发或同一次业务事件；频道不同、消息号不同、父分发为空。 |

## 修订记录

- r1 · 2026-11-27T05:27:00Z · baseline：按样本观测截止标注各条手记：C1/C2 收窄采用，C3 不得改写，C4/C6 待导入或待验收。
- r2 · 2026-11-27T18:22:06Z · receipt-imported：导入迟来重投回执 1→2：仅抬升旧链为双源交叉，新消息 #2 / 分发 3 的处置不变（仍为未处理）。
- r3 · 2026-11-27T18:22:06Z · supplement-accepted：验收补充材料：字段级同构成立，链路级不成立，业务级留给人核对；未并入告警本体。
