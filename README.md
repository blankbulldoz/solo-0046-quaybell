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
| `/channels/{channel}/groups/{group}/pause` | POST | `paused` 为布尔值；`paused:true` 同时开始/关联一轮排空，可带 `timeout_seconds` |
| `/channels/{channel}/groups/{group}/drain` | GET / POST | 读取当前排空轮次与分类，或开始/重试排空；可带 `timeout_seconds` |
| `/channels/{channel}/groups/{group}/drain/retry` | POST | 超时后显式开启下一轮排空 |
| `/channels/{channel}/groups/{group}/drain/confirm` | POST | 值守员确认已结束轮次的分类结果 |
| `/channels/{channel}/groups/{group}/drain/resume` | POST | 显式确认提前恢复取件 |
| `/channels/{channel}/groups/{group}/drains` | GET | 历轮排空摘要与当前轮 |
| `/channels/{channel}/groups/{group}/drains/{round}` | GET | 按轮号（或记录号）读取某轮分类报告 |
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

### 升级前的确认式排空

升级消费者前，在操作台右侧“升级前排空消费者组”确认限时（或直接点“暂停取件”并确认）。这会把组置为暂停并开启一轮持久化的排空（`drain_rounds` 表，轮号按组单调递增），此后 `claim` 立即为空，但已发出的租约仍然有效：

```sh
curl -s -X POST http://127.0.0.1:8086/v1/channels/signal-weir/groups/oscillator/drain \
  -H 'Content-Type: application/json' -d '{"timeout_seconds":30}'
```

每个排空响应（`POST .../drain`、`GET .../drain`、`POST .../pause`）都带 `round`/`drain_round`（以及同义的 `epoch`、`generation`、`drain_id`）和三份给值守员的分类，条目同时给出 `id` 与 `delivery_id`，并附 `*_ids` 纯编号列表与 `counts`：

- `completed`（别名 `acked`/`done`）：本轮被正常确认、已完成的投递；
- `in_hand`（别名 `in_flight`/`leased`/`processing`/`outstanding`）：仍持有有效租约、尚在手上的投递，含 `worker`、租约截止和 `remaining_seconds`；
- `requeued`（别名 `retried`/`retry_ready`，其中 `expired` 单列自然过期）：租约自然到期后被回收、转回 `waiting` 等待重新领取的投递；
- `unclaimed`（别名 `successors`/`same_key_successors`）：从未领取的投递，包含同顺序键上仍在排队的后继；`waiting` 是当前所有 `waiting` 投递（含已重排重试者）。

没有常驻回收线程：读取 `GET .../drain` 会像取件一样先做一次组内有界回收，因此受控时钟下推进时间后再次读取，就能看到“自然过期 → 转未领重试”。当手上没有开放尝试时该轮为 `completed`；若到了 `deadline` 仍有在手租约，则轮次为 `timed_out`。

轮次归属与冲突语义：

- 排空未结束（`state:"draining"`）时直接 `POST .../pause {"paused":false}` 会得到 `409 conflict`（错误码 `drain_in_progress`），错误体在顶层、`details` 和 `drain` 中都带当前 `round` 与分类；界面会要求二次确认。
- 值守员确认提前恢复可重发 `{"paused":false,"confirm":true}`（也可回传 `"round":<当前轮>` 或调用 `POST .../drain/resume`），该轮标记为 `resumed` 并恢复发租约，响应仍指明对应轮号。
- 超时后重试排空：再次 `POST .../drain`、调用 `POST .../drain/retry`，或带 `{"retry":true}`，都会开启并返回**下一轮**（轮号 +1）；`GET .../drain` 只观察、不新建轮次，因此仍可读到 `timed_out` 的上一轮报告。

`python3 -m examples.walkthrough drain` 用受控时钟构造“一个正常确认、一个自然过期、一个尚未领的同键后继”，并校验提前恢复的 409 轮次归属与超时重试的新轮号。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough drain
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
