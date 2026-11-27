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

以下路径使用 `/v1` 前缀，写入和预查使用 `POST` 与 `application/json`。`{channel}`、`{group}` 和 `{id}` 用返回的标识替换。所有对外时间仍是 Unix 秒（浮点），与既有客户端和 `quaybell-trace/1` 证据合同保持一致；存储内部自 v2 起改为 Unix 整数毫秒（见下文“时间基准升级”）。

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

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

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
python3 -m examples.upgrade
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。

## 时间基准升级（v1 秒浮点 → v2 整数毫秒）

内部持久化在 schema v2 中统一为 Unix 整数毫秒，对外合同（HTTP JSON、`quaybell-trace/1` 证据）仍按 Unix 秒浮点解释，由边界层显式换算，客户端无需改动。

- **升级方式**：打开 `user_version=1` 的旧库时自动执行分阶段迁移，无需清空数据库。每一步在独立事务内提交并登记 `migration_ledger`；进程在任意一步后被打断，下次启动会跳过已完成步骤、从断点继续，最后才把 `user_version` 升到 2。
- **同一毫秒的观察边界**：旧秒值用 `ROUND(x*1000)` 映射到最近毫秒。两条旧数据若落在同一毫秒（亚毫秒差异被收缩），它们的先后仍由原有整数投递/尝试 ID 决定；就绪、租约截止与保留判定继续使用 `<= now` 的封闭区间，因此“恰好在截止时刻”的语义与升级前一致。升级既不拉长也不缩短任何租约或保留窗口超过半个毫秒。
- **历史可比性**：`quaybell-trace/1` 的格式、字段名和秒级单位不变，摘要仍可独立校验；单个时间戳相对旧值的偏差不超过 0.5 ms，消息状态、派生分发、尝试顺序和 ID 完全保持。已导出的旧证据继续按其文件内的秒值解释，不会被追溯改写。
- **对账**：迁移在改写前记录每张表的行数及各时间列的求和/极值清单，改写后换算回秒逐项核对（含 `foreign_key_check`），结果写入 ledger 的 `verify` 步骤；`python3 -m examples.upgrade` 用含毫秒内差异的待领时刻、仍有效的旧租约、刚到截止的签收和留存候选，连同中断续跑一起演示全过程。
