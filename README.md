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
| `/messages/{id}/audit` | GET | 发布时封存的标签、组规则、原回执与当前拓扑差异 |
| `/messages/{id}/receipt` | GET | 只取原发布回执；清理后仍可从审计凭证读回 |
| `/audit-receipts/{id}` | GET | 按审计凭证号（或原消息号）读取封存依据 |
| `/channels/{channel}/forensics` | GET / POST | 按消息号、生产键或审计期时间窗查询发布取证世代 |
| `/channels/{channel}/audit` | GET / POST | 读取或配置审计凭证开关与审计期秒数 |
| `/messages/{id}/evidence` | GET | `quaybell-trace/1` JSON 下载，摘要针对除 `sha256` 之外的规范化内容；消息清理后改返封存审计包 |
| `/channels/{channel}/retention` | GET / POST | 候选复核，或以 `{}` / `limit` 清理实际合格项 |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

## 发布回执取证

操作台的“预查”读取**当前**拓扑；发布后才创建的组也会出现在预查结果里，因此不能用它解释历史发布。发布事务现在会额外封存一份 `quaybell-publication-audit/1` 凭证，内容包括：

- 原发布回执 `original_receipt`（消息号、策略版、首次 fanout）；
- 当时信封的精确标签、规范信封摘要和策略版；
- 发布瞬间已存在的每个组及其标签规则、逐键比较结果和 `matched` 结论；
- `matched_groups=[]` 也会明确封存，因此“当时确实零匹配”和“查不到证据”是两种状态；
- 规范化内容的 `sha256`，读取时返回 `digest_matches`。

审计凭证保存在独立表中，不挂消息外键，也**不对 `(channel, producer, key)` 建唯一约束**。短保留清理删除原信封、回执、投递、尝试和消息级日志后，审计凭证仍在指定审计期内保留；同一生产键可发布新信封，审计查询会把不同消息号作为多个 `generations` 返回，不会把键永久占死。

审计期有两种用法：

1. 配置保留时长：建频道时传 `"audit": {"enabled": true, "retention_seconds": 2592000}`，或之后 `POST /v1/channels/{channel}/audit` 提交同样内容。默认启用并保留 90 天。超过 `audit_until` 后报告为 `audit_period_expired`，不再返回封存依据。
2. 按审计区间检索：`POST /v1/channels/{channel}/forensics` 可传 `{"start": 1730000000, "end": 1730000060}`、`{"key":"released-key"}` 或 `{"message_id":3}`；GET 形式支持 `?key=released-key&start=...&end=...`。

返回状态必须分开解读：

- `verified` / `evidence_status:"proven"`：发布时封存凭证仍在且摘要可复算，能证明原标签和组规则依据。`current_topology` 另外给出“若现在用旧标签预查”的结果，并列出发布后新建的组；它只用于显示差异，不是历史依据。
- `audit_credential_not_enabled` / `evidence_status:"unavailable"`：该历史发布没有启用审计凭证。若原消息仍在，接口会给 `evidence_status:"live_reconstruction"`，但明确标为非封存重建；若已清理，只能证明频道级 `pruned` 记录中的删除编号，`tags`、`rules` 和原 fanout 均为 `null`，不能用当前组反推。
- `audit_period_expired`：曾有凭证但已超出指定审计期，封存依据不再可读。

`python3 -m examples.walkthrough audit` 用受控时钟验收以下完整路径：审计启用前的历史记录、一次命中组、一次零匹配、短保留清理、清理后同生产键发布新信封并命中另一组、后来建组使当前预查失真、按生产键读取两代回执，以及无凭证历史记录的证据边界。

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
python3 -m examples.walkthrough audit
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
