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
| `/channels/{channel}/groups/{group}/takeover/preview` | GET | 接管前每项的当前组、状态与是否会转移 |
| `/channels/{channel}/groups/{group}/takeover` | POST | `new_group`、可选 `takeover_id`；幂等移交未完成项并终止旧租约 |
| `/channels/{channel}/groups/{group}/takeovers/{id}` | GET | 按接管单号或幂等键读取可对账结果 |
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

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

### 消费组接管

新版本消费者使用新组名时，可调用 `POST /v1/channels/{channel}/groups/{old-group}/takeover`，请求 `{"new_group":"consumer-v2","takeover_id":"rollout-1"}`。接管在一个写事务内完成：

- 旧组中 `waiting`、`leased` 的未完成分发会按原投递 ID 顺序复制到新组，并写入不可重复的 `旧投递 -> 新投递` 对账映射；顺序键保持不变，因此同键后继仍被新组队首挡住。
- 已 `acked` 的分发只出现在对账结果中并标记为 `acknowledged`，不会在新组重建，因而无需重做。`dead` 项不自动搬运，仍走显式 redrive。
- 在手旧租约的开放尝试记为 `superseded`，旧投递进入 `transferred`。旧进程晚到的 ack/nack 得到 HTTP 409、`reason="lease_superseded"`，响应和 `details` 带新组与新投递 ID；该确认不会改动新组状态。
- 同一 `takeover_id` 重试返回同一接管单；不带幂等键时，同源组和目标组的重复 POST 也复用最近接管单。重试不会创建重复分发，`summary.newly_transferred` 表示本次新补移交的条目数，报表会重算目标投递的最新状态。

`takeover` 工作流构造两个顺序键：第一个键有在手租约和后继，第二个键有已确认队首和待领后继；它展示接管前后每项归属、晚到确认的 409、重试结果，以及新组的领取顺序。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough takeover
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
