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
| `/channels/{channel}/groups` | POST | `name`、可选匹配 `tags`、可选 `mode="latest"`（或 `coalesce=true`）开启最新合并 |
| `/channels/{channel}/groups/{group}/pause` | POST | `paused` 为布尔值 |
| `/channels/{channel}/groups/{group}/mode` | POST | 修订 `mode`/`coalesce`，可选 `coalesce_timing` |
| `/channels/{channel}/groups/{group}/coalesce` | POST | 手动执行到期合并；也可携带模式字段兼作修订 |
| `/channels/{channel}/supersessions` | GET | 近期“旧投递被哪条新投递取代”的关系 |
| `/channels/{channel}/preview` | POST | 用发布信封查询当前标签匹配结果 |
| `/channels/{channel}/messages` | POST | `producer`、`key`、`lane`、`payload`、可选 `tags`、`delay_seconds` |
| `/channels/{channel}/groups/{group}/claim` | POST | `worker`、可选 `limit`，返回含 `lease.token` 的领取项 |
| `/deliveries/{id}/ack` | POST | `token` |
| `/deliveries/{id}/nack` | POST | `token` 和 `reason` |
| `/deliveries/{id}` | GET | 单条投递、尝试及取代关系 |
| `/sweep` | POST | `{}` 或 `limit`，按原截止时间回收全局租约 |
| `/channels/{channel}/dead` | GET | 最近 50 个失败投递及最后一次尝试 |
| `/deliveries/{id}/redrive` | POST | `{}`，使用当前策略追加一个后继投递 |
| `/messages/{id}` | GET | 信封、分发、尝试、消息级处理记录 |
| `/messages/{id}/evidence` | GET | `quaybell-trace/1` JSON 下载，摘要针对除 `sha256` 之外的规范化内容 |
| `/channels/{channel}/retention` | GET / POST | 候选复核，或以 `{}` / `limit` 清理实际合格项 |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

### 最新读数合并

只关心当前值的消费组可在建组时传 `"mode":"latest"`（也接受 `"merge_mode":"latest"`、`"coalesce":true`、`"conflation":true` 等兼容写法）；默认 `"mode":"all"` 的普通订阅继续收到每一条。合并范围是同一消费组、同一顺序键 `lane`；生产键 `key` 仍只负责发布幂等，不充当合并键。

可被取代的旧投递必须仍是 `waiting`、`attempt_count=0`，即从未被领走或尝试。已经发放租约的投递保持 `leased` 并正常确认/拒绝；拒绝或超时后即使回到 `waiting`，`attempt_count` 也已大于零，因此会照常退避、重试和进入死信，不会被合并掉。

默认时机 `coalesce_timing="ready"`：延迟中的新读数在自己的 `ready_at` 到达前不会取代任何旧消息，操作者仍能看到完整堆叠；到期后，下一次本组 `claim`、全局 `/sweep` 或显式 `/groups/{group}/coalesce` 会惰性执行合并。若同键还有一条更早的处理中/重试投递，最新投递仍受既有队首阻挡，必须等该义务按自身状态机处置；但中间从未尝试的待领项可先被标记为 `superseded`。如果业务明确要求“延迟项一入库就作废旧值”，可在建组或 `/mode` 时传 `coalesce_timing="publish"`。

被取代投递返回 `state:"superseded"` 和整数 `superseded_by`；新投递的轨迹中有 `supersedes: [旧投递号...]` 与 `supersession_links`（含双方消息号）。`GET /v1/messages/{id}`、证据导出、频道概览的 `supersessions` 和操作台消息轨迹/“合并取代记录”都可用于追查链路。取代是直接关系；连续合并时可沿新投递中的旧投递链接继续追溯。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough coalesce
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
