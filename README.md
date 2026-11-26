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
| `/messages/{id}/evidence` | GET | `quaybell-trace/1` JSON 下载，摘要针对除 `sha256` 之外的规范化内容；加 `start`/`end`（或 `from`/`to`、`window_start`/`window_end`）返回十分钟交班窗口件 |
| `/evidence/verify` | POST | 复核证据包摘要；窗口件还会复核所有事件时间是否符合声明窗口及窗外标记 |
| `/channels/{channel}/retention` | GET / POST | 候选复核，或以 `{}` / `limit` 清理实际合格项 |

默认策略是 `lease_seconds=24`、`retry_seconds=3`、`max_attempts=3`、`retention_seconds=3600`。重发原生产键时复用初始匹配结果，内容变化则返回 409。如果应用侧副作用必须去重，应按消息和消费组身份设计处理键。重投需要按其业务语义明确将后继身份识别为有意重新处理。

## 交班时间窗证据

事故审计不应拿走整条消息的内部处理包。对既有证据接口传入闭区间 Unix 秒边界即可取得 `quaybell-window-evidence/1` 窗口件，窗口最长 600 秒：

```sh
curl 'http://127.0.0.1:8086/v1/messages/42/evidence?start=1730310120&end=1730310720'
```

窗口件只把 `journal.at` 落在 `[start,end]` 内的发布、领取、确认、拒绝和实际回收事件放入 `trace.events`。它还区分两种容易混淆的时间口径：

- `journal.at = expired` 是租约被扫描/回收并写入处置的**实际时刻**；
- `attempts.expires` 是凭据失效的**截止时刻**。若截止时刻在窗内、实际回收在下一班，窗口中会有 `lease_deadline_reached` 时间事实，并明确给出 `reclamation_recorded_in_window=false`，但不会把它说成“已经回收”。

窗外事件只保留 `before_window` / `after_window` 关系、记录类型和规范化原记录的 SHA-256 承诺；窗外发布的生产者、生产键、标签和载荷不进入窗口件，只留下信封摘要。两次交班可分别导出相邻窗口，它们共享 `subject.message_id` 和 `envelope_commitment`，但各自只证明本窗口内的事实。

接收人不需要原整包即可在本地复核：

```sh
python3 -I quaybell/verify.py handoff-window.json
```

摘要口径与原证据一致：删除顶层 `sha256` 后，使用 `json.dumps(..., ensure_ascii=True, sort_keys=True, separators=(',', ':'))` 规范化，再对其 UTF-8 字节取 SHA-256。

也可把窗口件 POST 到 `/v1/evidence/verify`。复核会重算除 `sha256` 外规范化 JSON 的摘要、检查每个事件的真实时间是否在声明窗口内，并列出该窗口支持/不能支持的结论。直接改动窗内事实会得到 `digest_mismatch`；只重算外层摘要而保留旧的 `source_commitment` 还会得到 `source_commitment_mismatch`。若把窗外摘要标记改称为窗内，即使改动者重算自摘要，仍会得到 `outside_fact_claimed_inside` / `event_outside_window`。

随包 `sha256` 能发现复制损坏和未同步重算摘要的改动，但无秘密的自摘要不能阻止恶意导出者把所有承诺一起重算。需要对抗这种重新封装时，接收人应通过独立渠道钉住原始窗口件摘要：

```sh
python3 -I quaybell/verify.py window.json --expect-sha256 "$EXPECTED_WINDOW_SHA256"
```

`source.evidence_commitment` 另为将来比对完整 `quaybell-trace/1` 原包保留承诺；外部审计当下不需要原包。

可运行有界演示，构造“窗外发布、窗内两次领取、一个租约窗内到期但下一班回收、另一个租约下一个窗口确认”的同一消息两次交班，并生成篡改样本：

```sh
python3 -m examples.walkthrough window
python3 -m examples.window_demo --out /tmp/quaybell-window-demo
python3 -I quaybell/verify.py /tmp/quaybell-window-demo/handoff-1-claim-and-deadline.json
```

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
python3 -m examples.walkthrough window
```

每个工作流示例使用临时 SQLite 文件、受控时钟和本机随机端口，运行完即释放。可用 `python3 -m examples.walkthrough --help` 列出所有工作流。正式测试套件延后建设。
