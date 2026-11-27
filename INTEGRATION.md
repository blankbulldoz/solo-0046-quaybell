# Quaybell 接入指南：不可重复设备动作的确认、补做与人工核对

面向第三方接入方。场景：你通过 Quaybell 消费一条命令，去触发一次**不可重复**的设备动作（合闸、开锁、出料、扣款类物理动作）。消费者可能在“设备已动作之后、拿到 ACK 回执之前”退出；重启后租约可能还是原来那次，也可能已经轮换到下一次尝试；而一次人工 `redrive`（重投）在业务上代表“允许有意补做一次”。

本指南配套一个**无需外部依赖、可反复本机重跑**的真实 HTTP 示范：

```sh
python3 -m examples.device_handoff            # 四个场景全跑
python3 -m examples.device_handoff same-lease # 原租约：安全重发 ACK
python3 -m examples.device_handoff queryable  # 请求号可查：查询后确认或同号补做
python3 -m examples.device_handoff blind      # 盲下游：停止自动补做，交人核对
python3 -m examples.device_handoff redrive    # 人工 redrive：授权补做 / 已执行仅关闭
```

示范每次启动真实的 `RelayServer`（临时 SQLite、随机本地端口、受控时钟），用“丢弃 worker 对象 + 从 JSON 台账重新读取”模拟进程退出与重启，并用断言保证设备物理动作次数恰好符合预期。阅读本文时可对照 `examples/device_handoff.py`。

---

## 1. 先划清 ACK 的语义边界

这是整套接入最重要的一句话：

> **Quaybell 的 ACK 只表示“这个消费组对这条投递的处置结果已被中继记录”，它不是“设备已经执行”的证明。**

原因很直接：

- Quaybell 从不接触你的设备或下游，它无法知道物理动作是否发生。
- ACK 可以在调用设备**之前**就发出（错误实现），中继照样记录 `acked`。
- 反过来，设备可能已经动作，但 ACK 丢失、消费者退出，中继里该投递仍是未处置/超时状态。
- `GET /v1/messages/{id}` 与证据导出（`/evidence`）只证明**中继侧**发生过 `published / leased / acked / expired / nacked / redriven` 这些事件；证据里还会剥离租约 token。它们能用于审计中继行为，不能当设备执行回执。

因此“设备到底做没做”只能由以下三者回答，按可信度排序：

1. 设备/下游自己的记录（请求号查询、设备回执、维护日志）；
2. 人工现场核对结论 + 工单；
3. 你自己持久化的处理台账（它是你自己写入的事实，不是中继给的证明）。

中继给你的是 **at-least-once（至少一次）投递**和**幂等的处置回执读取**，不是 exactly-once 副作用。外部副作用的去重必须接入方自己设计。

---

## 2. Quaybell 侧的精确行为（本指南依赖的事实）

| 事实 | 说明 |
| --- | --- |
| 每次领取产生一次 **attempt** 和一个 **lease token** | `claim` 返回 `lease.number / lease.token / lease.expires`。token 只在响应里出现一次，服务端仅存摘要，之后的查询接口取不到它。**必须在调用设备前把 token 持久化到本地台账。** |
| ACK/NACK 可安全重试 | 用同一 token 再次请求：若该尝试已有相同处置，返回原回执并带 `"repeated": true`（HTTP 200）。这个“读已记录结果”的判断先于租约超时判断——**只要第一次 ACK 已落库，哪怕早就过了 `expires`，重放仍是 `repeated:true`，不会变成 409。** |
| ACK 未记录且租约已过期 | 旧 token 再 ACK 返回 `409 conflict`（`租约已处置或超时`）。token 摘要对不上则是 `409`（`该凭据不属于此投递`）。两种 409 都表示“这个旧凭据不能再处置了”，**不要无脑用同一 token 反复重试**，应进入第 5 节的对账。 |
| 下一次尝试有**新 token** | 租约到期后，组内下一次 `claim` 会先有界回收（reap）到期尝试：旧 attempt 记为 `expired`，投递按退避转为 `waiting`，到点后再次被领取，`lease.number` 加一，并签发**新 token**。也可用 `POST /v1/sweep` 全局回收。 |
| 尝试耗尽进入 **dead** | NACK 或超时使尝试数达到 `max_attempts` 时，投递变 `dead`。死信是终态，**不能直接 ACK**（再请求是 409）。 |
| `redrive` 是人工语义操作 | `POST /v1/deliveries/{id}/redrive` 只对 dead 投递生效；它追加**一条新的后继投递**：新 `delivery_id`、`parent_id=死信ID`、绑定当前最新策略、排在同一 `lane` 队尾。对同一死信重复 redrive 不新增后继：第一次 `201 {"repeated":false,...}`，之后 `200 {"repeated":true,"delivery_id":<同一个>}`。 |
| 顺序键（lane）阻挡 | 同一 `lane` 上更早的 `waiting/leased` 投递会挡住后面的投递；`acked/dead` 等终态不阻挡。建议把 `lane` 设为设备 ID，让同一设备的命令自然串行；一条悬而未决的命令会挡住该设备后续命令，这通常正是你要的。 |
| `pause` 不是租约保险箱 | 暂停只影响之后的 `claim`（且会跳过领取时的组内 reap），**不会延长已有租约**，也没有租约续约接口。人工核对耗时可能超过租约，按“租约已轮换/已 dead”路径处理。 |

默认策略 `lease_seconds=24, retry_seconds=3, max_attempts=3`。示范使用 `lease_seconds=10, retry_seconds=2, max_attempts=2` 以便受控时钟演示。

---

## 3. 接入方必须自己持有的两样东西

### 3.1 持久处理台账（durable ledger）

每个 `delivery_id` 一条（真实系统中放进与业务写入同事务的数据库；示范用原子替换的 JSON 文件）。至少包含：

```json
{
  "delivery_id": 17,
  "message_id": 91,
  "attempt": 2,
  "token": "本次尝试的 lease.token（机密，按本地凭据保护）",
  "previous_token": "第 1 次尝试的 token，用于先做安全重放探测",
  "request_no": "REQ-C-0001",
  "phase": "in_flight | device_done | unknown | ack_confirmed",
  "updated_at": 1730000013
}
```

写入顺序建议：

1. `claim` 成功 → **立刻**持久化 `delivery_id / message_id / attempt / token / request_no`（phase=`claimed`）；
2. 调用下游前，把 phase 推进为 `in_flight`（保证“我准备用这个请求号调用”已落盘）；
3. 下游返回明确成功 → phase=`device_done`；明确失败且确定未生效 → 记录可补做；超时/断连且无查询结论 → phase=`unknown`；
4. 只有 ACK 拿到 200 后才写 phase=`ack_confirmed`。

注意：Quaybell 的**生产键**（`producer` + `key`）只用于“重复发布同一信封时复用第一次发布回执”，它不是你给设备的请求号，二者不要混用。

### 3.2 下游请求号（request number / idempotency key）

- 对**可查询下游**：请求号按“业务命令”稳定生成（例如载荷里的 `command_id`，或 `message_id + ":" + 消费组` 派生），**同一 delivery 的所有自动尝试复用同一个请求号**。这样 `GET /executions/{request_no}` 能查到第一次到底做没做，下游也能按它幂等去重。
- 对**盲下游**：如果它连请求号幂等都不支持，请求号主要用于工单和日志关联；自动尝试在结果不明时**不得**再次调用。人工 redrive 后的补做建议使用绑定后继世代的新请求号（见第 7 节）。

---

## 4. 三个崩溃断点

```
claim ──(B0 台账落盘)──> 调设备 ──(B1 结果落盘)──> 发 ACK ──(B2 等回执/回执丢失)──> ack_confirmed
                ▲ 崩溃都可重启恢复；危险窗口是“设备已动作，但本地/中继都还没有终态”
```

- **B0：claim 后、调设备前退出。** 最简单：重启后租约内可直接 ACK？不——设备还没做。应当继续执行设备调用；请求号机制保证重复领取不会产生两次动作（可查询/幂等下游），盲下游则按第 5 节谨慎处理。
- **B1：设备已动作，但结果未及写入台账就退出。** 本地只看得到 `in_flight`，必须靠下游请求号查询或人工核对。
- **B2：设备已动作且（可能）已写 `device_done`，消费者在收到 ACK 回执前退出。** 这是题眼，分两种中继状态：
  - ACK 没到中继：原 token 重发 → `200 repeated:false`，ACK 现在被记录；
  - ACK 已到中继并落库、但回执在网络中丢失/进程没来得及处理：原 token 重发 → `200 repeated:true`，读到同一份已存回执。

两种情况都**只重发确认、不再调用设备**，因此都是安全的。示范 `same-lease` 用裸 socket 真实制造了“中继已记录 ACK、但响应从未被消费者读取”的 B2 丢失。

---

## 5. 核心决定表：什么时候重发确认、补做、交人

重启后的**第一个动作永远是用台账里的原 token 重放 ACK**（幂等、便宜、可能直接终结问题）。然后按下表分流。

| 重启后看到的事实 | 可按请求号查询的下游 | 超时后只知道连接断了的盲下游 |
| --- | --- | --- |
| 原 token ACK → `200 repeated:false/true`（租约可能仍有效，也可能 ACK 早已记录） | **安全重发确认**：结束，不再调用设备 | 同左（只要台账有 `device_done`，或 ACK 本就已记录） |
| 原 token ACK → `409`，重新 `claim` 得到 `attempt=n+1` 与新 token | 先用请求号查询：`executed` → **不再调用**，用新 token ACK；`无记录` → **可以补做**：用**同一请求号**再调（下游幂等），成功后 ACK；`pending/unknown` 超过 SLA → 交人 | 台账是 `device_done` → 可直接用新 token ACK；台账是 `unknown`（超时/断连）→ **禁止自动补做**，开人工核对工单，可 NACK 让其中止重试直至 dead，或保持等待 |
| 人工核对：设备**已执行** | 查询通常已能确认；以设备侧记录为准 ACK 当前尝试 | **可以安全确认**：凭人工工单 ACK 当前尝试的有效 token；若已 dead，则走 redrive 并在后继上只 ACK、不调设备 |
| 人工核对：设备**未执行**且明确授权补做 | 查询无记录时系统本就可同号补做；也可纳入人工 redrive 世代管理 | **必须且只能**凭人工授权对 dead 投递执行 **redrive**；后继是“有意重新处理”的许可，Worker 校验血缘+工单后补做一次 |
| 状态对不上（token 不属于投递、trace 与台账矛盾、工单缺失却出现 redrive 后继等） | **停止自动处理并交人核对**，不要猜、不要换 token 硬 ACK、不要直接再调设备 | 同左 |

三个“什么时候”的一句话版本：

- **可以安全重发确认**：同一尝试的同一 token（ACK 幂等）；或租约轮换后，已有“设备确实完成”的证据（请求号查询 `executed`、本地 `device_done`、人工“已执行”结论），此时用**当前尝试的新 token** ACK。
- **可以补做**：请求号查询证明下游无执行记录（用同一请求号）；**或者**人工现场确认未动作并授权、通过 redrive 产生了后继（把后继身份识别为有意重新处理）。
- **必须交人核对**：盲下游给出不明超时/断连且没有其他完成证据；查询返回长期 `pending/unknown`；或任何台账/中继/工单不一致。

如果**本地台账或 token 本身丢了**（磁盘损坏、token 未落盘），你将无法重放 ACK，也无法证明自己打算用哪个请求号：不要猜 token、不要直接再调设备。可查询下游若能从业务记录重建 `request_no`，就等租约过期后的下一次尝试，用查询结果决定确认或同号补做；盲下游一律按“结果不明”交人核对。这也是为什么第 3 节要求 claim 后、调用设备前先把 token 与请求号持久化。

---

## 6. 示范与 HTTP 报文对照

### 6.1 场景 `same-lease`：仍是原租约，安全重发 ACK

```text
claim → attempt=1, token=T1（落盘）→ 设备动作成功 → device_done → 崩溃（ACK 未发送）
重启（时钟未前进，租约仍有效）
POST /v1/deliveries/1/ack {"token":"T1"} → 200 {"outcome":"acked","repeated":false,...}
```

第二种命令演示“回执真的丢了”：示范用裸 socket 发出 ACK、不读响应，并通过 `GET /v1/messages/{id}` 看到 `acked` 事件已落库后关闭连接。重启后：

```json
POST /v1/deliveries/1/ack {"token":"T1"}
200 {"attempt":1,"delivery_id":1,"outcome":"acked","state":"acked","repeated":true,...}
```

把受控时钟再拨过几千秒，重放结果仍是 `repeated:true`——已记录回执的读取不检查租约是否过期。

真实报文示例（来自本机 `python3 -m quaybell --port 8099`）：

```text
POST /v1/channels/device-gate/groups/actuators/claim
200 {"items":[{"delivery_id":1,"message_id":1,"lease":{"number":1,
 "token":"TONH3W4elKoNBuBM-Ux5IQGSSeYkNk2a","expires":1795736588.8}, ...}]}

POST /v1/deliveries/1/ack  → 首次
200 {"attempt":1,"delivery_id":1,"outcome":"acked","state":"acked","repeated":false,...}
同一请求立刻重试
200 {"attempt":1,"delivery_id":1,"outcome":"acked","state":"acked","repeated":true,...}
```

### 6.2 场景 `queryable`：请求号可查

时钟拨过租约与退避后，旧 token 得到：

```text
POST /v1/deliveries/1/ack → 409 {"error":"conflict","message":"租约已处置或超时"}
claim → attempt=2，新 token T2
```

- 命令 `REQ-Q-DONE`：`GET /executions/REQ-Q-DONE`（你的下游接口，示范中是内存查询）返回 `executed` → **不再次调用设备**，用 T2 ACK。
- 命令 `REQ-Q-MISSING`：查询返回无记录 → 用**同一个** `REQ-Q-MISSING` 再调一次，下游执行；再用同号调用会得到 `duplicate_skipped`，物理动作不重复。

### 6.3 场景 `blind`：只知道连接断了

`BlindDevice` 没有 `query()`。第一次调用抛出的错误在“设备其实已动作”和“请求根本没到设备”两个隐藏分支下**完全相同**（`AmbiguousOutcome`，模拟超时/连接重置）。租约轮换到 attempt 2 后，参考 worker：

1. 不开设备调用；
2. 在 `HumanDesk`（示范的持久工单）开 `open` 核对单；
3. 输出 `[必须交人核对]`。

随后人工从设备现场/维护终端确认“已执行”并登记工单，对账程序用 attempt 2 的 token ACK，全程没有第二次设备调用。

### 6.4 场景 `redrive`：人工重投才是补做许可

当人工核对来不及在尝试预算内完成，worker 可以对最后一次尝试 NACK（或等待租约超时回收），投递进入 `dead`：

```text
POST /v1/deliveries/2/nack {"token":"<T2>","reason":"..."}
200 {"attempt":2,"outcome":"nacked","state":"dead","repeated":false,...}
```

人工到现场确认设备**未动作**，在工单系统签发 `redo_authorized` 后，运维入口才允许调用：

```text
POST /v1/deliveries/2/redrive {}
201 {"source_id":2,"delivery_id":3,"policy":1,"repeated":false}

POST /v1/deliveries/2/redrive {}   （重复人工点击/网络重试）
200 {"source_id":2,"delivery_id":3,"policy":1,"repeated":true}
```

后继被 `claim` 后，参考 worker 做两项校验才执行设备：

- `GET /v1/messages/{message_id}` 中该后继 `parent_id=2`，且事件序列含 `redriven`（中继血缘）；
- 工单系统中存在针对 `parent_id=2` 的 `redo_authorized` 审批（业务许可）。

示范里补做使用新请求号 `REQ-REDO-A#redo-3`：盲下游无法告诉你旧请求号是否卡在未知去重状态，新号码保证这次“被授权的补做”不会被旧状态吞掉；而“第一次物理上没动作”已由人工现场核对关闭了双动作风险。补做成功后 ACK 后继，设备物理动作总数恰好为 1。

另一条分支 `4b` 演示“核对后发现其实已执行、但投递已经 dead”：仍需一次 redrive 来获得可处置的后继，但后继读到的工单结论是 `executed`（非 `redo_authorized`），于是**只 ACK、不调用设备**。也就是说：redrive 是中继侧唯一的“追加后继/有意重新处理”机制，**要不要在后继上真正再次动作，由绑定的人工结论决定**；不要把“存在 redrive 后继”本身当成无条件重做命令。

---

## 7. 人工核对与 redrive 运维规程（建议固化为脚本/权限）

1. **告警/工单**：出现 `unknown` 超时、409 后无法对账、查询长期 pending、死信时，自动建单，字段至少含 `channel/group/delivery_id/message_id/request_no/attempt 历史/最近错误`。
2. **带外核对**：值班人员通过设备面板、设备厂商查询接口、现场摄像头/到位检查确认物理结果，把结论写成 `executed | not_executed | indeterminate`，记录核对人与证据。
3. **按结论处置**：
   - `executed` 且仍有有效尝试 → 用台账/新 claim 的当前 token ACK；
   - `executed` 但已 dead → 走 redrive，后继凭 `executed` 工单只 ACK；
   - `not_executed` 且允许补做 → 双人/有权限账号签发 `redo_authorized`，再调用 redrive；后继校验“血缘 + 工单”后补做一次；
   - `indeterminate` → 保持冻结，联系设备厂商/拆机核查，**宁可暂停该 lane（设备）也不要赌一次**。
4. **权限分离**：让“核对人”和“redrive 执行人/审批人”符合你的变更规范；示范中的 `operator_redrive()` 在缺少 `redo_authorized` 工单时直接抛 `ApprovalRequired`，可以作为运维脚本的最小护栏。
5. **保留证据**：Quaybell 的 `/messages/{id}/evidence` 导出绑定观察时间与快照、带可复算 `sha256`，适合与人工工单编号一起归档。再次提醒：它证明的是中继事件，不是设备动作。

---

## 8. 用常驻实例手工验证（curl）

```sh
# 用独立空库，示例中的 delivery_id 才会从 1 开始；也可换成 python3 -m quaybell --demo
mkdir -p /tmp/quaybell-guide
python3 -m quaybell --port 8086 --db /tmp/quaybell-guide/demo.sqlite3
B=http://127.0.0.1:8086/v1

# 1) 建策略与消费组
curl -s -X POST $B/channels -H 'Content-Type: application/json' -d '{
  "name":"device-gate",
  "policy":{"lease_seconds":10,"retry_seconds":2,"max_attempts":2,"retention_seconds":3600}}'
curl -s -X POST $B/channels/device-gate/groups -H 'Content-Type: application/json' \
  -d '{"name":"actuators","tags":{}}'

# 2) 发命令（request_no 放在 payload，由你自己的服务解释）
curl -s -X POST $B/channels/device-gate/messages -H 'Content-Type: application/json' -d '{
  "producer":"device-bridge","key":"cmd-0001","lane":"valve-7","tags":{},
  "payload":{"command_id":"C-0001","request_no":"REQ-C-0001","op":"close"}}'

# 3) 领取并记下 delivery_id 与 lease.token（落盘！）
curl -s -X POST $B/channels/device-gate/groups/actuators/claim \
  -H 'Content-Type: application/json' -d '{"worker":"bridge-1"}'

# 4) 设备动作成功后 ACK；Ctrl-C 杀掉消费者也没关系，用同一 token 重跑本请求即可
curl -s -X POST $B/deliveries/1/ack -H 'Content-Type: application/json' \
  -d '{"token":"<lease.token>"}'

# 5) 看完整轨迹与尝试（响应不含 token）
curl -s $B/messages/1 | python3 -m json.tool
```

要观察租约轮换：停掉消费者等待 10 秒租约 + 2 秒退避，再执行一次第 3 步的 claim，会得到 `"number":2` 和新 `token`；旧 token 的 ACK 返回 409。要观察死信与重投：对第 2 次尝试 NACK（或继续不 ACK 等超时），投递变 `dead`，再 `POST /v1/deliveries/{id}/redrive`。

---

## 9. 上线检查清单

- [ ] claim 后、调用设备前，已把 `delivery_id / attempt / token / request_no` 写入持久存储。
- [ ] 重启恢复的第一步是“用原 token 幂等重放 ACK”，并正确处理 `repeated:true` 与 `409`。
- [ ] 自动尝试复用稳定请求号；可查询下游在新尝试上**先查询再决定**调用或确认。
- [ ] 盲下游的不明超时/断连不会触发自动二次调用，而是产生人工核对单。
- [ ] 只有在“查询已执行 / 本地 device_done / 人工已执行结论”之一成立时才 ACK 一个你没有在本次尝试里完成的投递。
- [ ] redrive 被当作受控的人工操作：有核对结论、有授权、Worker 校验 `parent_id/redriven` 血缘与工单。
- [ ] 后继使用请求号策略明确（可查询下游可同号先查；盲下游用 `原号#redo-<后继delivery_id>` 新世代号）。
- [ ] `lane` 按设备/资源设置，理解队头阻挡与暂停语义；没有把 pause 当租约续期。
- [ ] 监控 `dead`、`/channels/{c}/dead`、租约超时率与人工工单时长。
- [ ] 文档与值班口径一致：**ACK 不是设备执行证明**；设备事实以下游查询/现场核对为准。

### 常见反模式

- “收到 200 ACK 就告诉第三方设备已执行”——错误，ACK 只代表中继记录了你的处置。
- 重启后不查台账，直接重新 claim 并再次调用设备——对不可重复动作可能造成二次物理动作。
- 收到旧 token 的 409 后立刻用新尝试的 token ACK，而不核对设备结果——可能在设备未执行时错误关闭命令。
- 把自动重试/租约轮换当成“可以再做一次”的许可——at-least-once 只保证会再投递，不保证副作用可重复。
- 没有人工工单就把 redrive 接口开放给自动化故障恢复——redrive 的补做语义必须由业务授权注入。
