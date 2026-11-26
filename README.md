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
| `/channels/{channel}/batches` | POST / GET | 建立设备批次（`producer`、可选 `key`、`lane`、`tags`），或列出批次 |
| `/channels/{channel}/batches/{batch}` | GET / PUT / POST | 查询批次与分片；PUT 按客户端引用幂等建立；POST 可带 `action`（`seal`/`cancel`） |
| `/channels/{channel}/batches/{batch}/shards` | POST | 上传/重发一个分片：`index`（或 `part_number`/`sequence`）与 `payload`，同号同内容复用、异内容 409 |
| `/channels/{channel}/batches/{batch}/shards/{index}` | POST / PUT | 同上，分片号在路径中 |
| `/channels/{channel}/batches/{batch}/seal` | POST | 封口：把全部分片物化成一条消息并按当前标签生成分发；重复封口返回同一回执 |
| `/channels/{channel}/batches/{batch}/cancel` | POST | 取消批次（幂等），不留下顺序键屏障 |
| `/channels/{channel}/batches/{batch}/receipt` | GET / POST | 取回已封口回执，供网络重发后核对 |

`{batch}` 既接受返回的数字 `batch_id`，也接受建批时提供的字符串 `key`/`batch_key`；同样的接口在 `/v1/batches/{batch}/...` 下也可用。`parts`、`chunks` 是 `shards` 的别名，`complete`/`commit` 是 `seal` 的别名，`abort` 是 `cancel` 的别名，`uploads`/`intakes` 是 `batches` 的别名。

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 批次收件：分片上传、封口与取消

设备批次可以分多次上传。建批后把每个分片 `POST .../batches/{id}/shards` 暂存，**封口前不会产生任何消息或投递**，因此消费者领不到半成品，批次也不会占据顺序键。

- **重启续传**：批次和分片都持久化在 SQLite。进程重启（甚至杀掉进程后用同一个 `--db` 文件再起）后，`GET .../batches/{id}` 仍给出 `open` 状态和已收分片；用相同分片号和相同内容重发会得到 `repeated=true` 的同一记录，随后可继续补齐并封口。
- **夹发普通消息的次序**：批次只在封口事务里物化成一条消息并生成分发，所以两片之间发进同一 `lane` 的普通消息拥有更小的投递号，消费者会**先领到并确认该普通消息，再领到整批**。这一先后可由回执的 `fanout` 投递号、领取顺序和 `GET /messages/{id}` 轨迹复验。封口载荷默认是 `{"batch_id", "part_count", "parts":[...]}`（同时以 `shards`/`items`/`chunks` 暴露同一有序数组），领取项也带 `batch_id` 与 `parts`。
- **取消不阻键**：`POST .../cancel` 把批次置为 `cancelled`（重复取消幂等）。暂存期从未写过投递，所以取消后该 `lane` 上的普通发布会立即成为队首并可领取；对已取消批次封口返回 409。
- **回执取回**：封口返回与普通发布同形的回执（`message_id`、`policy`、`fanout`、`repeated`）。网络超时后可重发 `seal`（返回 `repeated=true` 和同一个 `message_id`/`fanout`），或 `GET .../batches/{id}/receipt` 取回。

有界工作流 `python3 -m examples.walkthrough batch` 在临时库上真实起停服务并模拟一次实例重启，断言上述四件事。

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough export
python3 -m examples.walkthrough batch
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
