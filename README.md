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
| `/channels/{channel}/groups/{group}/claim` | POST | `worker`、可选 `limit`、可选 `partition`/`partitions`，返回含 `lease.token` 与 `assignment_version` 的领取项 |
| `/channels/{channel}/groups/{group}/partitions` | GET / POST | 核对分片版本与每键归属；POST 用 `partitions`、`workers` 或显式 `assignment` 重新分配，可带 `expected` |
| `/channels/{channel}/groups/{group}/assignments` | GET / POST | 上一行的别名；`/assignment`、`/topology`、`/rebalance`、`/assign` 同义 |
| `/channels/{channel}/groups/{group}/ownership` | GET | 每顺序键当前属主、实际处理进程、状态与租约代际；可带 `?worker=` 只看某进程 |
| `/channels/{channel}/groups/{group}/authorize` | POST | `worker`、`lanes`（或 `keys`），逐键返回是否为当前有权进程 |
| `/channels/{channel}/groups/{group}/fence` | POST | `{}` 或 `{"all":true}`，立即围栏失权（或全部旧代际）的开放租约并把债务放回队首 |
| `/channels/{channel}/groups/{group}/members` | GET / POST | 成员与最近心跳；POST `{worker}` 登记/心跳，`/join` 可同时提交分片配置 |
| `/channels/{channel}/partitions` | GET / POST | 频道视角：各消费组的分配版本与分片属主；POST 可带 `group` 定位消费组 |
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

## 固定分片与可重新分配投递

一个消费组内，多个工作进程可以各接固定分片。顺序键（发布信封里的 `lane`，即“顺序键”）经稳定的 `sha256(lane) mod partitions` 落到一个分片；每个分片在任一分配版本（`version`，也称 `generation`）下只有一个属主进程。取件请求仍发到同一个 `claim` 入口，中继按当前归属过滤：每个进程只会领到自己分片上的同键队首，响应和租约都带 `partition`、`owner` 与 `assignment_version`。未配置分片的消费组保持基线行为，任何 `worker` 都可按同键队首规则取件。

```sh
# 两个分片，两个进程各接一个（i % workers 轮配；也可用 strategy=range 或显式 assignment）
curl -s -X POST http://127.0.0.1:8086/v1/channels/device-hall/groups/pool-a/partitions \
  -H 'Content-Type: application/json' \
  -d '{"partitions":2,"workers":["proc-1","proc-2"]}'

# 操作者核对版本与每键当前归属（属主、实际持约进程、状态、租约代际）
curl -s http://127.0.0.1:8086/v1/channels/device-hall/groups/pool-a/assignments
curl -s 'http://127.0.0.1:8086/v1/channels/device-hall/groups/pool-a/ownership?worker=proc-2'

# 只允许当前属主领取；越权指定不属于自己的 partition 返回 403
curl -s -X POST http://127.0.0.1:8086/v1/channels/device-hall/groups/pool-a/claim \
  -H 'Content-Type: application/json' -d '{"worker":"proc-1","limit":10}'

# 从两个分片调到三个：expected 做乐观版本核对；可给新进程 proc-3 让出分片
curl -s -X POST http://127.0.0.1:8086/v1/channels/device-hall/groups/pool-a/rebalance \
  -H 'Content-Type: application/json' \
  -d '{"partitions":3,"workers":["proc-1","proc-2","proc-3"],"expected":1}'
```

演练“两分片调三分片、热键仍有旧版租约和重试、另一键已空闲”时，规则如下：

- **空闲键立即迁走。** 重新分配在一个写事务里递增版本并改写属主。没有开放租约的顺序键（已确认或已死信）立刻由新属主领取，不需要等任何租约到期，也不需要推进时钟。
- **热键债务优先，后继不能抢跑。** 跨版本仍由既有的“同顺序键最小未处置投递挡住后继”保证顺序。属主交接后，新进程第一次领到的一定是那条旧债务投递（同一 `delivery_id`，尝试号续增、`assignment_version` 为新版本），处置完后才领得到后继消息；其他进程或其他分片不受影响。
- **单一有权进程与代际围栏。** 默认 `lease_policy:"move"`：当某顺序键在新版本改属其他进程时，其中尚未闭合的旧版租约会在再分配事务内被围栏（旧 `token` 再确认/拒绝返回 409），旧债务立即回到队首由新属主接管；归属未变的进程可继续凭原租约处置到截止。需要旧式平滑交接时传 `"lease_policy":"honor"`，旧凭据在到期前仍有效，交接走退避/回收。`POST .../fence` 可随时显式围栏失权租约，`{"all":true}` 围栏全部旧代际租约。
- **可核对。** `GET .../assignments`（别名 `partitions`、`topology`）返回 `version/generation`、分片属主表和 `keys[]` 每键归属：`owner` 是当前有权属主，`serving_worker`/`open_attempt` 是实际持约进程与其租约代际，`state` 区分 `idle/leased/expired/retry/delayed/ready/blocked`，`handoff` 提示 `lease_bound/fenceable/ready_for_owner/idle`。消息轨迹 `GET /messages/{id}` 的尝试与 `lease_fenced` 事件也记录分片代际。浏览器操作台中部新增“分片归属与版本”面板，可直接提交再分配、围栏并查看每键归属。

完整的有界演练见 `python3 -m examples.walkthrough partitions`：它用受控时钟跑通“领取→拒绝退避→旧版再次持约→扩到三分片并改属→围栏旧凭据→空闲键零等待迁移→新属主先偿旧债再取后继→版本/归属/授权核对”。

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough partitions
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
