# Quaybell 消息中继

Quaybell 是供内部服务集成使用的本机消息代理。生产者经 HTTP 发布结构化消息，消费组领取带租约的分发。浏览器操作台用于观察顺序阻挡、确认消息、处置死信和复核清理对象。

## 启动

环境需要 Python 3.11 或更高版本及 Python 自带的 `sqlite3`，无需安装包。执行以下命令，浏览器访问 `http://127.0.0.1:8086`。

```sh
python3 -m quaybell --demo
```

示例在所选数据库首次创建 `signal-weir` 时加载两组和四条消息。再次启动会保留状态。省略 `--demo` 从空工作区开始，使用 `--db 路径` 指定独立状态库或 `--port 端口` 更换端口。监听固定在本机回环地址。SQLite 文件及其工作文件默认存于 `.quaybell/`，请为运行用户保留同一目录的写权限。

左栏选择频道和消费组，中栏查看分发、消息和死信，右栏发布、取件、修订策略或清理。点击投递号可以检查该消息下的各组尝试并下载证据。浏览器关闭时，本次页面领到的租约凭据会随页面关闭丢弃，租约到时可回收。演示模式的延迟和租约使用实际系统时钟。

## HTTP 接口

以下路径使用 `/v1` 前缀，写入和预查使用 `POST` 与 `application/json`。`{channel}`、`{group}` 和 `{id}` 用返回的标识替换。所有时间是 Unix 秒。

| 路径 | 方法 | 输入或结果 |
| --- | --- | --- |
| `/channels` | GET / POST | 枚举频道或用 `name`、可选 `policy` 建立频道 |
| `/channels/{channel}` | GET | 当前策略、分组计数、队列样本、近期消息与死信 |
| `/channels/{channel}/policy` | POST | 全部四个策略值和预期版号 `expected` |
| `/channels/{channel}/groups` | POST | `name`、可选匹配 `tags` |
| `/channels/{channel}/groups/{group}/pause` | POST | `paused` 为布尔值 |
| `/channels/{channel}/preview` | POST | 用发布信封查询当前标签匹配结果 |
| `/channels/{channel}/messages` | POST | `producer`、`key`、`lane`、`payload`、可选 `tags`、`delay_seconds`；同键异信返回带 `review` 复核文档的 409 |
| `/messages/{id}/resend` | POST | `{}`，按已存储的原告警信封重发，稳定复用原回执 |
| `/channels/{channel}/candidates` | GET / POST | 列出复核候选，或用发布信封加可选 `candidate_key` 建立候选新键单（只暂存、不发布） |
| `/channels/{channel}/candidates/{id}` | GET | 最终信封、指纹、当前/实际去向、原告警状态与绑定的稳定回执 |
| `/channels/{channel}/candidates/{id}/confirm` | POST | 可选 `fingerprint`、`key`；在单写事务内重校键与去向并发布，回执和候选单永久绑定，重复确认返回同一回执 |
| `/channels/{channel}/candidates/{id}/discard` | POST | 放弃未确认候选；放弃后不能发布 |
| `/channels/{channel}/groups/{group}/claim` | POST | `worker`、可选 `limit`，返回含 `lease.token` 的领取项 |
| `/deliveries/{id}/ack` | POST | `token` |
| `/deliveries/{id}/nack` | POST | `token` 和 `reason` |
| `/sweep` | POST | `{}` 或 `limit`，按原截止时间回收全局租约 |
| `/channels/{channel}/dead` | GET | 最近 50 个失败投递及最后一次尝试 |
| `/deliveries/{id}/redrive` | POST | `{}`，使用当前策略追加一个后继投递 |
| `/messages/{id}` | GET | 信封、分发、尝试、消息级处理记录 |
| `/messages/{id}/evidence` | GET | `quaybell-trace/1` JSON 下载，摘要针对除 `sha256` 之外的规范化内容 |
| `/channels/{channel}/retention` | GET / POST | 候选复核，或以 `{}` / `limit` 清理实际合格项 |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 409 后的复核、重发与候选确认

生产者误把刚用过的键填给另一条告警时，`POST /messages` 仍保持基线约定：同信封重发返回 `repeated:true` 和原稳定回执，同键异信封返回 `409`。现在 409 体会额外带 `review` 文档，操作台自动打开复核对话框，明确区分两条路径：

- **重发旧告警**：按钮提交的是服务端存储的原信封（也可直接 `POST /v1/messages/{id}/resend`），因此一定复用原 `message_id` 与最初 fanout 回执，不会把表单里的新内容误发出去。
- **确实是新告警**：用 `review.suggested_key`（形如 `原键-2`，只按已提交消息计算、不预留）建立 `publication_candidates` 候选复核单。建单只暂存最终信封与指纹，**不写 messages/deliveries，任何未确认候选都不会被发布**；操作台右栏会列出未确认候选。

确认步骤 `POST /channels/{ch}/candidates/{id}/confirm` 把三者关联到同一条候选记录并在 `GET .../candidates/{id}` 的 `binding` 中返回：

1. **最终信封**：候选单保存规范化信封与 `fingerprint`；确认可回传 `fingerprint`，不一致返回 409 `envelope_mismatch`。
2. **实际去向**：确认事务内按当前订阅重新匹配并写投递，记录 `fanout`（组名 → 真实投递 ID），而不是只采用建单时的预演组名。
3. **稳定回执**：首次确认得到的 `{message_id,policy,fanout}` 写入候选单；之后无论串行还是并发重复确认，都返回同一回执（`repeated:true`）。

两个标签不同的新告警可以同时持有同一个 `candidate_key`（候选表不对此加唯一约束）。并发确认时，写事务 `BEGIN IMMEDIATE` 串行化并配合 `messages(channel,producer,dedup_key)` 唯一约束：恰好一个成功，另一个收到带新 `review.suggested_key` 的 409；败者可改用下一个键再次确认，候选单仍有效。若等待期间原告警被保留清理，候选单保留建单时的快照，`original_state` 变为 `pruned`，界面提示旧回执已失效；确认会生成全新回执（若刻意改用已释放的原键，会标记 `reuses_released_key`）。

端到端有界工作流覆盖了上述“两个不同标签告警争用同一候选新键、原告警其间被清理”的场景：

```sh
python3 -m examples.walkthrough review     # 别名 conflict / republish / candidates
```

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough review
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
