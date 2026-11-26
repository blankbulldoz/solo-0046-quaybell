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
| `/channels/{channel}/retention` | GET / POST | 候选复核；`{}` / `limit` 立即清理，或用 `plan_id` / `ids` 确认具体名单 |
| `/channels/{channel}/retention/plans` | GET / POST | 列出冻结计划，或按当前候选创建带编号的冻结名单 |
| `/channels/{channel}/retention/plans/{plan_id}` | GET | 计划详情、状态、批准记录与删除/跳过结果 |
| `/channels/{channel}/retention/plans/{plan_id}/apply` | POST | 批准并按冻结名单执行；重复或并发批准只生效一次，其余返回同一结果（`repeated:true`） |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 保留清理的名单审批

值守页“保留复核 · 名单审批”按截图审批的实际节奏设计：

1. **查看候选**：`GET .../retention` 仍只给当前时刻的候选，不产生计划。
2. **生成批准计划**：`POST .../retention/plans` 把当时的编号名单和观察时刻冻结并落库，返回 `plan_id` 与 `status:"prepared"`。
3. **批准并执行**：`POST .../retention/plans/{id}/apply`（可带 `approved_by`）只处理冻结编号，不会把预览后才到期的新消息连带删除。

执行时在同一个写事务内逐条复查：消息已不存在记为 `not_found`；仍有 `waiting`/`leased` 分发或开放尝试记为 `active_delivery`/`active_attempt`；其中死信刚被重投出后继分发的，记为 `reason:"redriven"` 并附上活跃 `delivery_id`，**跳过该消息而不是级联删掉活跃分发**；尚未到期记为 `not_due`。响应和 `GET .../plans/{id}` 给出 `removed`（已删编号，附别名 `deleted`）、`skipped`/`changed`（变化跳过明细）以及 `status:"applied"`、`approved_by`、`approvals` 批准记录。

计划结果持久保存，因此管理员重试同一请求会读到首次结果并带 `repeated:true`。两人同时批准时，SQLite 写事务（`BEGIN IMMEDIATE`）把执行串行化：恰好一人的请求真正删除，另一人在锁后读到 `applied` 结果，同样得到 `repeated:true`，两次批准人都留在 `approvals` 中。旧的 `POST .../retention` 带 `{}`/`limit` 仍按当前时刻立即重算清理；带 `plan_id`、`ids`/`message_ids` 或回贴 `eligible` 行时则走确认名单语义。

下面的工作流用受控时钟和两个真实 HTTP 线程，完整演示“新到期不连带、重投保分发、管理员重试、两人同时批准”：

```sh
python3 -m examples.walkthrough approval
```

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough approval
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
