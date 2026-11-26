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
| `/channels/{channel}/groups` | POST | `name`、可选匹配 `tags`；可同时传 `"mode":"confluence"` 与 `time_budget_seconds` 开启来源序号合流 |
| `/channels/{channel}/groups/{group}/pause` | POST | `paused` 为布尔值 |
| `/channels/{channel}/groups/{group}/confluence` | GET / POST | 读取合流缺口，或配置 `mode="sequence"`、`sources`、`time_budget_seconds` |
| `/channels/{channel}/groups/{group}/confluence/records` | POST | 提交 `source`、`sequence`、可选 `payload`；同源同号幂等，异源同号分别归属 |
| `/channels/{channel}/groups/{group}/confluence/advance` | POST | `{}` 或受控测试中的 `now`；跳过超预算缺号并释放连续项。取件也会惰性执行 |
| `/confluence-records/{id}` | GET | 全局可追查记录、晚到事件与隔离状态 |
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

### 多采集链合流

当两条或更多各自声称序号连续的采集链进入同一消费组时，可用 `POST /channels/{channel}/groups/{group}/confluence` 开启 `"mode":"sequence"`，并约定 `time_budget_seconds`。身份是 `(source, sequence)`，不是单独的序号：

- `GET .../confluence` 的 `gaps` / `missing_sequences` 按来源列出当前缺号，`gap_report` 还包含首次发现时间、预算截止时间和 `open`、`filled`、`budget_exceeded` 状态。
- 缺号之后的同源记录进入 `buffered`，不会越过缺口；其他来源的连续记录照常 `released`。在预算内补到缺号会把缺口标记为 `filled`，并按同源序号连续释放缓存。
- 调用 `.../confluence/advance`（或在预算到期后第一次 claim）会把超预算缺口标记为 `budget_exceeded/skipped`，随后释放该来源缓存的后续连续项。Quaybell 没有后台轮询线程，因此推进是惰性且在写事务内完成的。
- 同源同号重复提交返回 `duplicate:true`、`repeated:true` 和原记录 ID，不新增计数。两来源恰好使用相同序号时，两条记录都保留，分别归属各自 `source`，并通过 `peer_record_ids` 标明跨源同号；这不是重复。
- 已跳过预算缺口后才到达的同源项不能回插已推进的流。它会成为 `status:"late"`、`trace_status:"traceable"` 的可追查/隔离记录，进入 `late_arrivals` 与 `incidents`，并由 `/confluence-records/{id}` 或组内 `.../confluence/records/{local_id}` 追踪。需要人工追平时可调用纯领域对象的 `reconcile_late(id)`，把它作为显式追平项追加到输出。

也可以在普通发布信封中加 `source` 和 `sequence`；已开启合流的组会把对应投递挂到序号闸门，未释放的 `buffered`/`late`/`duplicate` 投递不会被 claim。独立领域 API 位于 `quaybell/confluence.py`，可用注入时钟做确定性测试。

验收用例固定在工作流 `confluence`：

```sh
python3 -m examples.walkthrough confluence
```

它构造 A 链的 1、3（缺第 2 项）和仍持续前进的 B 链。预算内快照为 `{"chain-a":[2],"chain-b":[]}`，B1～B4 已释放；推进 5 秒预算后 A3 释放、A2 缺口记为超预算；此时 A2 晚到，返回 `late` 与 `/v1/confluence-records/7` 追查地址，轨迹中的事件为 `late_arrival_traceable`，事件状态为 `traceable`。同源 A1 重发返回原 ID 的重复回执，而 A1/B1 同序号是两个不同 ID 且互相记录为 peer。

以下载荷可发布到示例频道。默认延迟为零。

```json
{"producer":"sensor-gate","key":"pulse-next","lane":"coil-north","tags":{"feed":"telemetry"},"payload":{"reading":48,"unit":"mV"}}
```

## 构建与生产工作流

```sh
python3 -m compileall -q quaybell examples
python3 -m examples.walkthrough claim
python3 -m examples.walkthrough confluence
python3 -m examples.walkthrough retention
python3 -m examples.walkthrough export
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
