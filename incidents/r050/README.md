# R050 诊断：同一载荷为何能留下“两条消费者日志”

> 范围：只做本地、合成、可复跑的历史对照。脚本在临时 SQLite 和 `127.0.0.1` 随机端口上运行，使用固定受控时钟；**不会连接现场，也不会向合作方重发消息**。

## 1. 先给结论

仅凭题面中的两类残证，**不能在以下三个假设中做唯一选择**：

1. **新的发布**：第二个生产身份（或清理后复用的旧生产键）产生新 `message_id` / `delivery_id`。
2. **旧分发重试**：同一个 `delivery_id` 出现更大的 `attempt`（Quaybell 字段为 `lease.number`）。
3. **重投形成新分发**：死信源分发不变，重投追加一个新的 `delivery_id`，其 `parent_id` 指向源分发，新分发的第一次尝试仍是 `attempt=1`。

设备号相同、载荷摘要相同、半夜时间接近，以及一张**尚未和第二条日志的分发 ID/时间连接起来的重投回执**，都只能提高怀疑，不能排除另外两种历史。

决定性的补证顺序是：

1. 先让合作方补第二条原始日志中的 **`delivery_id`、`attempt/lease.number`、`message_id`、worker、精确时间戳**；
2. 同时让持有重投回执的值班员补 **回执来源环境/频道、执行时间、操作人、`source_id` / `delivery_id` / `repeated`**，并找清理前 evidence 导出或数据库备份；
3. 再由库 owner/DBA 核对幸存的 `journal(kind='pruned')`、SQLite/备份和证据导出；
4. 最后由生产者/网关负责人用 outbox、发布回执和访问日志确认是否存在第二个生产身份。

## 2. 一键复跑

在仓库根目录执行：

```sh
python3 incidents/r050/reproduce.py
python3 -m compileall -q quaybell examples incidents/r050/reproduce.py
```

脚本会构造三段主历史和一段清理后生产键复用对照，并把机器可读结果写入：

```text
incidents/r050/transcript.json
```

输出中的三个核心坐标为：

```text
A_old_delivery_retry : M1 / D1 / A2
B_redrive_new_delivery: M1 / D2 / A1, D2.parent_id=D1
C_new_publication    : M2 / D2 / A1, D2.parent_id=NULL
```

重复运行生成的 `transcript.json` 内容稳定；脚本内有 17 项 `verification_checks`，并对关键 ID、SHA-256、清理后的级联删除结果和证据摘要做断言。

## 3. 题面表象的固定投影

合成载荷的规范化摘要为：

```text
payload_sha256=f4135e7d751c3afe5bb0aed276580c541e6135b60896136c96bf410ecc1e8720
```

脚本按“第一条是带 ID 的日志，第二条是只有设备/摘要的日志”构造。投影如下：

| 顺序 | 时间（+08:00） | 题面可见字段 |
| --- | --- | --- |
| 1 | 2026-11-27 00:02:11 | `delivery_id=1`, `attempt=1`, `device_id=DEV-24601`, `payload_sha256=f413…8720` |
| 2 | 2026-11-27 00:04:37 | 只有 `device_id=DEV-24601`, `payload_sha256=f413…8720`；中继 ID 缺失 |

如果现场两条日志的时间顺序与上述相反，应先用可信时间戳排序，再使用同一套 ID 比较规则；关键是给“只有摘要的那一条”补出中继坐标。

> 注意：上面的摘要是本诊断使用 `sha256(quaybell.codec.pack(payload))` 得到的，即键排序、紧凑 JSON。现场必须向合作方确认其摘要的规范化算法；算法不一致时，摘要不能直接比较。

## 4. 三段都能留下该表象的历史

### A. 旧分发重试：`M1/D1/A1` 后再领取为 `M1/D1/A2`

脚本路径：`worlds.A_old_delivery_retry`。

1. `00:01:15` 发布 `M1`，消费组得到原始分发 `D1`。
2. `00:02:11` 第一次领取：`D1 / attempt=1`，形成合作方第一条带 ID 日志。
3. 第一次尝试被 `nack`；策略 `retry_seconds=120`，`D1` 回到 `waiting`。
4. `00:04:37` 再次领取：仍是 `D1`，但 `attempt=2`。合作方第二条日志没有打印这些 ID，只剩设备和摘要。
5. 第二次尝试 `ack`，随后保留期到期，执行清理。

清理前谱系：

```text
D1 parent_id=NULL, state=acked, attempts=[1 nacked, 2 acked]
```

这说明：**同一个分发号 + 更大的尝试号**是旧分发重试的决定性特征。

脚本还在另一频道生成了一张真实但无关的重投回执（`source_id=2 -> delivery_id=3`），用来说明“值班员有一张回执”本身不能证明它属于本事件。

### B. 重投形成新分发：`D1` 死信后追加 `D2(parent=D1)`

脚本路径：`worlds.B_redrive_new_delivery`。

1. 策略设为 `max_attempts=1`。
2. 发布 `M1` 并得到 `D1`；`00:02:11` 领取为 `D1 / attempt=1`，即第一条日志。
3. 该尝试 `nack` 后，`D1` 立即进入 `dead`。
4. `00:02:25` 值班执行重投，回执为：

   ```json
   {"source_id": 1, "delivery_id": 2, "policy": 1, "repeated": false}
   ```

   重复调用同一重投接口只会返回 `repeated=true` 和同一个 `D2`，不会再创建第三个分发。
5. `00:04:37` 领取后继：`D2 / attempt=1`，仍属于 `M1`。第二条业务日志缺少 `D2`，因此无法把值班员的回执和它连接起来。

清理前谱系：

```text
D1 parent_id=NULL, state=dead,  attempts=[1 nacked]
D2 parent_id=1,    state=acked, attempts=[1 acked]
journal event: redriven details = {source_id:1,delivery_id:2,...}
```

在这段历史里，回执**确实对应**第二次处理；但在只有题面残证时，无人能证明这一点。

### C. 真正的新发布：新生产键生成 `M2/D2/A1`

脚本路径：`worlds.C_new_publication`。

1. 第一次发布使用生产键 `gw-24601-20261127-0001`，得到 `M1/D1`，随后领取并确认。
2. 脚本先验证：清理前用**同一 producer + 同一 key + 完全相同信封**重发，只会得到 `repeated=true`，仍返回 `M1/D1`，不会新增分发。
3. `00:03:50` 网关用第二个生产键 `gw-24601-20261127-0002` 再次发布完全相同的载荷，得到 `M2/D2`。
4. `00:04:37` 领取为 `D2 / attempt=1`；第二条日志仍只有设备和摘要。

清理前谱系：

```text
M1 -> D1(parent NULL, acked)
M2 -> D2(parent NULL, acked)
```

因此，仅仅看到“新分发号 + attempt=1”还不能区分 C 和 B；必须再看 `message_id`、`parent_id`、`redriven` 事件或生产者发布台账。

### D. 对照：清理后同一生产键可以变成“新的发布”

脚本路径：`controls.production_key_reuse_after_cleanup`。

Quaybell 删除旧消息后，`(channel, producer, dedup_key)` 的唯一约束随记录一起消失，原生产键可以复用。对照实验中：

```text
第一次 key=reusable-key -> M1/D1
pruned 删除 M1
再次同一 key=reusable-key -> M2/D2, repeated=false
```

所以：

- **清理前**，同 producer/key/同信封的重试不会形成新分发；
- **清理后**，仅凭“生产键相同”不能排除新发布；
- 必须同时比较发布时间、`pruned` 时间、`message_id` 和发布回执。

## 5. 清理后库里还剩什么

三段主历史都实际调用了保留清理。清理后的目标频道计数均为：

```text
messages=0
deliveries=0
attempts=0
message_level_journal=0
```

原因是 `messages` 删除会级联删除 `deliveries`、`attempts` 以及带 `message_id` 的消息级事件。

但频道级清理记录没有 `message_id`，不会随消息删除；脚本中可查到类似：

```json
{
  "kind": "pruned",
  "at": 1795716000,
  "details": {"message_ids": [1]}
}
```

这条记录能证明“哪些消息 ID 在何时被清理”，但**不含设备号、载荷摘要或分发 ID**。单独拿到它仍不能判定机制；它的价值在于和可信日志时间、生产回执中的 `message_id` 对齐。

## 6. 补证排除力矩阵

前提：补证来自可信日志系统、中继库/备份或签名/链路完整的导出，并且覆盖了相关时间窗。残缺或来源不明的“没有找到”只能提高怀疑。

| 补到的证据 | 排除新的发布？ | 排除旧分发重试？ | 排除重投新分发？ | 准确结论 |
| --- | --- | --- | --- | --- |
| 设备号相同、载荷摘要相同、时间接近 | 否 | 否 | 否 | 三种历史均复现；只说明业务载荷同一。 |
| 一张未连接到第二条日志 ID/时间的重投回执 | 否 | 否 | 否 | 只提高重投怀疑。回执体只有 `source_id/delivery_id/policy/repeated`，没有设备、载荷、消息号、频道或时间。 |
| 第二条原始日志补为同一个 `D1`，且 `attempt>=2` | **是** | 否，且支持它 | **是** | 旧分发重试。新发布和重投都会产生新的 `delivery_id`。 |
| 第二条原始日志补为同一个 `D1`、同一个 `attempt=1` | **是** | **是**（真正的再次领取会变成 A2） | **是** | 三种“新消息/旧重试/重投后继”均不符合；改查同一尝试内的消费者重复处理、日志重复或 ack 回执重试。 |
| 第二条原始日志补为新的 `D2`、`attempt=1`，但没有 `message_id/parent_id` | 否 | **是** | 否 | 可排除同一旧分发重试；不能区分新发布与重投后继。 |
| 可信 trace/evidence 显示两次处理共享 `M1` | **是** | 否 | 否 | 新发布被排除；还要看 `delivery_id/attempt/parent_id` 区分重试与重投。 |
| `M1` 的 trace/evidence 显示 `D2.parent_id=D1`、存在 `redriven` 事件，且第二条日志就是 `D2` | **是** | **是** | 否，且确认它 | 重投形成新分发。 |
| 生产者 outbox/发布回执显示第二个生产身份生成 `M2`，fanout `D2` 与第二条日志一致 | 否，且确认新发布 | **是** | **是（对已知 D1 的重投谱系）** | 真正的新发布；若记录不完整，仍要排查未知第三消息。 |
| 可信时间证明第二条日志晚于删除 `M1` 的 `pruned` 事务，且现网库没有用清理前备份回滚 | **否，反而支持新发布/其他来源** | **是（M1 已删除，不能再领取）** | **是（M1/D1 谱系已级联删除）** | 旧 M1 的重试或重投在该库状态下不可能；转向清理后新发布、键复用或其他系统。 |
| 两次领取的 policy 版本不同，且 ID 行/补证属于同一分发 | 否 | **是** | 否 | 同一 delivery 绑定的策略版本不会变；新发布和重投都可能绑定当前策略。版本相同则不能排除重投。 |
| 相同 worker/IP、报错文案或调用栈 | 否 | 否 | 否 | 租约超时、nack、重试换 worker、重投都可能产生相同或不同执行位置；不是身份证据。 |
| evidence JSON 重算 `sha256` 一致 | 否 | 否 | 否 | 只能证明文件未损坏/与摘要自洽；SHA-256 不是外部签名，仍需证明导出由可信对象库或值班终端提供。 |
| “在某份日志/导出里没找到第二条” | 通常不能直接排除 | 取决于范围与 ID | 取决于范围与 ID | 只有在记录完整、时间窗正确、ID 明确且快照覆盖该消息时，缺失才有排除力；否则只是怀疑。 |

### 重投回执的具体判定规则

拿到回执后，先记录回执体中的：

```text
source_id, delivery_id, policy, repeated
```

然后按以下顺序判断：

1. **回执的 `delivery_id` 出现在第二条原始日志中**，且清理前 trace/evidence 证明该 D 的 `parent_id=source_id`：可确认重投，并排除新发布和旧重试。
2. **只能证明 `source_id=第一条日志的 D1`**：这说明 `D1` 曾成为死信并有一个后继，但仍未证明匿名第二条就是后继。若策略允许 `max_attempts>1`，第二条日志也可能是死信前的另一次旧尝试；必须比较重投操作时间、尝试时间和第二条日志时间。
3. **回执来源环境/频道不明**：它可能像脚本 A/C 中的回执一样属于 `other-desk` 或另一套部署；此时没有排除力。
4. **回执标有 `repeated=true`**：表示重复请求读取了已有重投结果，不会产生第二个后继。

## 7. 应该先找谁、补什么

### 第一优先级（并行）

实际顺序建议是：**先在内部找持有重投回执的值班员固化原件与来源（沟通成本最低），同时向合作方日志管理员发出只读取证清单；真正打开判定缺口的是合作方补出的第二条日志中继 ID。**

#### 找合作方值班/日志管理员

明确要求：**不要重发、不要回放、不要再次调用业务接口**。只导出午夜窗口的原始日志和采集元数据：

- 未被格式化裁剪的原始行；
- 可信时间戳、时区、时钟来源；
- `message_id`；
- `delivery_id` / 分发号；
- `attempt` 或 `lease.number`；
- `lease` 到期时间、worker/pod/IP；
- consumer group、channel、trace-id/request-id；
- 日志采样、过滤和保留策略；
- 载荷摘要使用的规范化算法。

最低目标是把“只有设备号和摘要”的那一条补成 `(message_id, delivery_id, attempt)`。

#### 找持有重投回执的值班员

要求提供：

- 回执原件、截图或完整响应体；
- HTTP 状态码和 `repeated` 字段；
- 来源机器、SQLite/服务环境、频道和消费组；
- 执行重投的精确时间、操作人、工单/控制台记录；
- 当时是否下载过 `quaybell-trace/1` evidence JSON；
- 终端下载目录、对象存储、工单附件和备份位置。

这一步内部协调成本低，应立即做；但最终与第二条日志连接仍依赖合作方补出的 D2/时间。

### 第二优先级：Quaybell 库 owner / DBA / 备份负责人

要求：

- **立即复制并冻结当前 SQLite 文件、WAL/SHM 和现有备份**；在取证完成前不要再清理、VACUUM、覆盖或重启写入。
- 查询幸存清理记录：

  ```sql
  SELECT id, at, kind, details
  FROM journal
  WHERE channel = ? AND kind = 'pruned'
  ORDER BY id;
  ```

- 查找清理前的数据库快照、存储卷快照和 evidence 导出。
- 若找到清理前库或导出，核对：

  ```sql
  SELECT m.id AS message_id, m.producer, m.dedup_key, m.published,
         d.id AS delivery_id, d.parent_id, d.state, d.policy,
         a.number AS attempt_no, a.outcome, a.started, a.finished
  FROM messages m
  JOIN deliveries d ON d.message_id = m.id
  LEFT JOIN attempts a ON a.delivery_id = d.id
  WHERE m.channel = ?
  ORDER BY m.id, d.id, a.number;
  ```

- evidence 导出可通过重算去掉 `sha256` 后的规范 JSON 校验摘要；同时要记录文件来源和保管链。

已清理的在线 `GET /v1/messages/{id}` 会是 404，这与题面前提一致；不要把 404 误判为“消息从未存在”。

### 第三优先级：生产者/网关负责人

要求提供同一设备号、同一摘要在时间窗前后的：

- outbox/发件箱记录；
- 每次发布的 `producer` 和 `key`；
- Quaybell 发布回执中的 `message_id`、`repeated`、`fanout`；
- HTTP 访问日志、响应状态和请求幂等键；
- 清理作业时间与生产键复用记录。

重点判断是否存在第二个 key/producer，或清理后用旧 key 重新发布。

### 第四优先级：下游业务/副作用负责人

请他们提供业务唯一键、数据库唯一约束、外部调用幂等号和两次副作用的提交时间。业务去重记录可以证明“两次外部效果”是否真实发生以及发生顺序，但通常不能单独证明中继侧属于重试还是重投；仍需中继身份坐标。

## 8. 现场安全注意

- 本目录脚本中的所有 POST 都发往本机临时实例，是合成取证/对照，不是现场重发。
- 对现场只应执行既有日志收集、文件复制、备份读取和只读 SQL/GET 查询。
- 不要为了“看看会不会再重复”而调用发布、重投、ack/nack 或清理接口。
- 在证据未闭合前，临时结论应写为：

> 当前残证与“新发布、旧分发重试、死信重投后继”三种历史均相容；未连接的重投回执只提高重投怀疑。优先补合作方第二条日志的 `message_id/delivery_id/attempt` 与回执来源/时间，再用清理前 trace/evidence 或发布台账做唯一排除。
