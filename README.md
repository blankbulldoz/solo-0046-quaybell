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
| `/channels/{channel}/backfills` | GET | 列出本频道补订交接单及未决条目数 |
| `/channels/{channel}/groups/{group}/backfills` | POST | 按 `name`、时间窗和标签发起可重试的补订交接与对账 |
| `/channels/{channel}/backfills/{id}` | GET | 交接单对账结果、缺口条目和封口状态 |
| `/channels/{channel}/evidence` | POST | 上交 `quaybell-trace/1` 证据包，校验摘要后入库 |
| `/channels/{channel}/backfills/{id}/evidence` | POST | 上交证据包并直接核销同编号缺口 |
| `/channels/{channel}/backfills/{id}/resolve` | POST | 对未归类缺口做 `accepted` / `skipped` 人工裁决 |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

## 补订交接与删除对账

归档组可用 `POST .../groups/{group}/backfills` 按标签和时间窗补订最近一小时的消息，交接期间正常发布和领取不中断。系统只依据三类现存材料下结论：

- **仍在库的原消息**：补订一条本组投递，按原始发布时间整块前插到交接期新发布之前；
- **有据删除**：清理在删除前把顺序键、标签和信封摘要写入频道清理记录；值班员在清理前导出的证据包经 SHA-256 核验通过后，缺口记为 `recovered_evicted` 并撤除屏障；
- **未归类删除**：只剩清理编号、拿不到原信封的删除先记 `unclassified` 并在真实顺序键上立一个不可领取的屏障投递。未人工 `resolve` 前，热键后继不能越过这个仍欠的前序；冷键（无缺口的顺序键）不被拖住，可继续领取。

交接单按 `(频道, 消费组, name)` 幂等：创建响应丢失时用同一身份重发，返回同一份交接单和当前对账，不会重复建投递。证据包重发返回原记录；摘要不符返回 409。所有缺口进入终态（`delivered`、`recovered_evicted`、`accepted`、`skipped`）后交接单 `sealed=true`。投递在顺序键内改由逻辑位置 `position` 排序，使补订前序稳定先于后继。

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
python3 -m examples.walkthrough backfill
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。`backfill` 工作流在真实 HTTP 实例上演示两类删除（有据/未归类）、同键新发布、创建响应丢失重试，以及冷键独立、热键后继不越前序的对账与继续领取。正式测试套件延后建设。
