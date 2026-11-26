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
| `/deliveries/{id}/ack` | POST | `token`，可选 `result`（`unit`、`amount`，可附供应商明细） |
| `/deliveries/{id}/nack` | POST | `token` 和 `reason` |
| `/channels/{channel}/result-rule` | POST | 结果对账规则四元组与预期版号 `expected` |
| `/sweep` | POST | `{}` 或 `limit`，按原截止时间回收全局租约 |
| `/channels/{channel}/dead` | GET | 最近 50 个失败投递及最后一次尝试 |
| `/deliveries/{id}/redrive` | POST | `{}`，使用当前策略追加一个后继投递 |
| `/messages/{id}` | GET | 信封、分发、尝试、消息级处理记录 |
| `/messages/{id}/evidence` | GET | `quaybell-trace/1` JSON 下载，摘要针对除 `sha256` 之外的规范化内容 |
| `/channels/{channel}/retention` | GET / POST | 候选复核，或以 `{}` / `limit` 清理实际合格项 |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

## 处理结果对账

旧版消费者的 ACK 只带 `token`，新版可在同一 ACK 上附加 `result`。两个供应商可能把同一台设备的产量分别报成整克（`{"unit":"g","amount":1200}`）或千克小数（`{"unit":"kg","amount":"1.200"}`）。中继按频道内**版本化的对账规则**把上报量归一化成基准单位下的整数刻度（用有理数精确换算，不使用二进制浮点），再比较刻度是否相等：

- 归一化后等量即视为同一结果。丢响应后换单位、调整键顺序或数字写法重交，都读回第一次的回执（`repeated=true`），不会因序列化差异误报冲突。
- 界面取整后看起来相同、但刻度不同的重交返回 409，并同时给出已记录量与重交量，**不**当作重复；原承诺不被改写。
- 租约已处置或过期后才送达的结果返回 409，消息中解释被拒收的归一化量，且不写入任何结果事实。
- 每次结果都钉住接收时的规则版号。规则经 `/result-rule` 升级后，历史 ACK 仍按旧版解释，新投递使用新版；回执与证据因此能说明当时为何接收或拒绝。
- `amount` 只接受整数或普通十进制字符串（拒绝浮点、科学计数法）；无法落到规则刻度精度的数值返回 400。

供应商在 `result` 内附加的原始明细只整体参与摘要计算，用于内部完整性核对；归一化刻度、单位、规则版号和决定原因可对外，原始明细与摘要不进入 `/messages/{id}/evidence` 证据导出。证据还内嵌当时生效的规则定义，并在导出前做一次脱敏红线校验。

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
python3 -m examples.walkthrough reconcile
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
