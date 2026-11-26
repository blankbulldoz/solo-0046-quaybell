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
| `/channels/{channel}/messages` | POST | `producer`、`key`、`lane`、`payload`、可选 `tags`、`delay_seconds` |
| `/channels/{channel}/groups/{group}/claim` | POST | `worker`、可选 `limit`，返回含 `lease.token` 的领取项 |
| `/deliveries/{id}/ack` | POST | `token` |
| `/deliveries/{id}/nack` | POST | `token` 和 `reason` |
| `/sweep` | POST | `{}` 或 `limit`，按原截止时间回收全局租约 |
| `/channels/{channel}/dead` | GET | 最近 50 个失败投递及最后一次尝试 |
| `/deliveries/{id}/redrive` | POST | `{}`，使用当前策略追加一个后继投递 |
| `/messages/{id}` | GET | 信封、分发、尝试、消息级处理记录 |
| `/messages/{id}/evidence` | GET | `quaybell-trace/1` JSON 下载，摘要针对除 `sha256` 之外的规范化内容 |
| `/channels/{channel}/retention` | GET / POST | 候选复核，或以 `{}` / `limit` 清理实际合格项 |
| `/channels/{channel}/groups/{group}/replays` | POST | `window_from`、`window_to`，可选 `limit`，按失败事实建立重演会话 |
| `/channels/{channel}/replays` | GET | 枚举此频道的重演会话及计数 |
| `/replays/{id}` | GET | 会话窗口依据、回放项、原消息/原尝试引用与生产侧对照 |
| `/replays/{id}/claim` | POST | `worker`、可选 `limit`，只从本会话回放队列发放租约 |
| `/replay-items/{id}/ack` / `nack` | POST | 与生产投递相同形态的凭据和重试语义 |
| `/replays/{id}/close` | POST | `{}`，关闭会话并固化关闭时生产侧快照 |
| `/replays/{id}/evidence` | GET | `quaybell-replay/1` JSON 下载，摘要针对除 `sha256` 外的规范化内容 |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

## 故障重演会话

重演会话供复盘使用：操作者选定频道、原消费组和 Unix 秒窗口，服务按**失败事实**选入回放项。每个符号事实是所选组里原尝试的失败时刻——`nacked` 取尝试完成时刻，`expired` 取租约截止时刻——落在 `[window_from, window_to]` 内的尝试；同一原投递只取窗口内最后一条失败尝试，按失败时刻与原投递 ID 升序。窗口与消息发布时间、投递就绪时间无关，因此迟发消息和跨窗口的失败都能按事实区分。会话响应里的 `window.basis = "attempt_failed"` 及 `window.basis_text` 是选入依据的权威说明。

会话创建是单次写事务：它保存原消息、原投递与原尝试的只读引用（原消息之后被清理也不会丢失），并以创建时的频道策略为回放队列的租约、退避和次数上限。会话一经创建即为隔离的回放队列，`/replays/{id}/claim` 只处理本会话的项，与生产组 `claim` 完全分离；同一消费组同一时间窗口最多一个打开的会话，重复创建返回 409，关闭后可以以相同窗口另开新会话。

回放项可以单独领取、确认或拒绝；拒绝按当前回放策略退避并重新就绪，次数用尽转死信，确认与拒绝的相同请求读回原回执。关闭会话必须没有打开的尝试；关闭固化各回放项状态和关闭时的生产侧快照（消息数、投递与尝试计数），会话随后只读。会话详情里的 `production` 给出创建时基线、当前值（以及关闭后的关闭时值），`GET /replays/{id}/evidence` 导出 `quaybell-replay/1`，其中包含回放项引用、会话事件、`production.baseline`、`production.at_close`、`production.current` 和 `production.delta`。这些计数均基于回放项持有的原消息与尝试 ID 之外的生产事实，便于在关闭后核对生产侧数量没有因回放而变化。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
