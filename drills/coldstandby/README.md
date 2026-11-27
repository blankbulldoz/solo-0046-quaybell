# Quaybell 冷备与放行演练

本目录是一套**可重跑**的冷备、恢复、验收演练，全部走 Quaybell 的公共 HTTP 接口与
SQLite 在线备份 API，不依赖第三方包。三个工具脚本只用标准库，统一以 `python3 -I`
运行（隔离模式，不加载当前目录/环境变量中的模块，避免受信问题）。

## 演练要回答的问题

同一频道有两份冷备，**消息条数与最大消息号完全相同**，只有一份是在「死信已重投、
后继尚未处置」时取的。放行方手上只有事故前留存的两份事实：

1. **重投回执**：`POST /deliveries/{id}/redrive` 的响应，含 `source_id`（死信投递）
   与 `delivery_id`（重投后继投递）；
2. **另一条在手租约的取件事实**：`claim` 响应，含 `delivery_id`、`message_id` 与
   `lease.token` / `lease.expires`。

演练要在「恢复到全新目录、原租约可能已到期」的现场，区分三种结论：

| verdict | 含义 | 退出码 |
| --- | --- | --- |
| `reject_wrong_backup` | 选错备份：重投回执指向的后继在恢复库中不存在 | 2 |
| `hold_reaped_after_restore` | 恢复对了，但取件回收推进了在手租约状态 | 3 |
| `release_clean_restore` | 恢复点正确、后继未处置且无回收推进，可放行 | 0 |

并在「另一条消息有同键（同顺序键/lane）后继」时重复整套演练。

## 一键演练

```sh
# 从仓库根目录运行；默认每次先清空 runtime/，保证可重复
python3 -I drills/coldstandby/run_drill.py
# 自定义工作目录
python3 -I drills/coldstandby/run_drill.py --workdir /tmp/qb-drill
# 复用已有工作目录（不清空）
python3 -I drills/coldstandby/run_drill.py --keep
```

预期输出（两个场景 × 三个案例，共六条）：

```
[a-single-lane]            case-1-wrong-backup:             reject_wrong_backup (rc=2)
[a-single-lane]            case-2-correct-but-reaped:       hold_reaped_after_restore (rc=3)
[a-single-lane]            case-3-correct-clean-release:    release_clean_restore (rc=0)
[b-same-lane-successor]    case-1-wrong-backup:             reject_wrong_backup (rc=2)
[b-same-lane-successor]    case-2-correct-but-reaped:       hold_reaped_after_restore (rc=3)
[b-same-lane-successor]    case-3-correct-clean-release:    release_clean_restore (rc=0)
```

- `a-single-lane`：死信后继与在手租约在不同 lane；
- `b-same-lane-successor`：同一 lane 放两条消息并各自死信重投，制造**同键后继互相
  阻挡**（队头未处理时后继 `cause=blocked`）。

## 单步可重跑命令

下面的路径以一次 `run_drill.py` 产出的 `runtime/` 为例；实际值守时把路径换成本机
状态库与备份目录即可。

### 1. 备份（在线一致性快照，服务可同时运行）

```sh
python3 -I drills/coldstandby/quaybell_backup.py \
  --db runtime/a-single-lane/live/quaybell.sqlite3 \
  --out runtime/a-single-lane/backups \
  --label backup-correct \
  --note "重投之后、后继未处置的正确冷备"
```

产物：`<label>.sqlite3`（已 checkpoint、DELETE 日志模式的自包含快照）、
`<label>.manifest.json`（计数/状态/谱系指纹）、`<label>.sha256`。

两份备份「消息条数与最大消息号相同」可直接比对清单：

```sh
jq '.summary.messages' runtime/a-single-lane/backups/backup-early.manifest.json \
                       runtime/a-single-lane/backups/backup-correct.manifest.json
# 两份输出完全一致；差异只在 deliveries 谱系（early 无 successor，correct 有）
```

### 2. 恢复到全新空目录（校验和 + 完整性，拒绝覆盖非空目录）

```sh
python3 -I drills/coldstandby/quaybell_restore.py \
  --backup   runtime/a-single-lane/backups/backup-correct.sqlite3 \
  --manifest runtime/a-single-lane/backups/backup-correct.manifest.json \
  --target   /tmp/qb-restored-case3
```

校验和不匹配或目录非空会直接拒绝（退出码 1）。恢复报告写在
`/tmp/qb-restored-case3/restore-report.json`。

### 3. 起恢复实例并验收

```sh
python3 -m quaybell --db /tmp/qb-restored-case3/quaybell.sqlite3 --port 8091
# 另一个终端
python3 -I drills/coldstandby/quaybell_acceptance.py \
  --base http://127.0.0.1:8091/v1 \
  --channel cold-drill --group rotor \
  --redrive-receipt runtime/a-single-lane/facts/redrive-receipt.json \
  --lease-fact      runtime/a-single-lane/facts/lease-fact.json \
  --evidence-dir    /tmp/qb-restored-case3/acceptance-evidence
echo "verdict exit=$?"
```

## 判定依据（为什么能区分三种结论）

验收工具严格遵循「**先固定取件前证据，再执行取件**」，证据目录里 `01`–`08` 全部
是取件前落盘，`09` 才是 claim 响应：

1. **选错备份**：用重投回执的 `delivery_id` 去恢复库 `/channels/{c}/dead` 找死信条目的
   `redriven_as`。重投前的备份里死信还在但 `redriven_as=null`，谱系对不上 →
   `reject_wrong_backup`，且**不执行任何取件**。
2. **回收推进**：消息级审计里的 `expired` 事件是回收的可靠信号，即使同一次 claim
   先 reap 又立刻重新领取（最终状态回到 `leased`），事件计数仍会增加。取件前已有
   `expired`（到手即被早先回收处置）或本次取件新增 `expired`，都判
   `hold_reaped_after_restore`。
3. **干净放行**：谱系核对通过、重投后继取件前为 `waiting` 且 `attempt_count=0`、
   无任何 `expired` 事件 → `release_clean_restore`。

### 关于在手租约凭据的安全处理

- 只用取件事实里的 `lease.expires` 与服务器 `observed_at` 比对判断是否到期；
- **仅当证据表明已到期**时，才用原 token 发一次 ack，预期收到 409 作为旁证
  （`08-stale-token-ack-result.json`）；租约仍有效时跳过，绝不主动改写现场；
- 证据导出接口 `/messages/{id}/evidence` 本身去除租约凭据，已验证导出 JSON 不含
  token；完整 token 只存在于 `00-input-lease-fact.json` 与 `09-claim-response.json`。

## 同键后继的阻挡证据

`b-same-lane-successor` 场景下，取件前的 `07-lane-heads-before-claim.json` 显示同
lane 两条重投后继：队头 `cause=ready`，后继 `cause=blocked` 且 `head_id` 指向队头；
claim 只返回队头，证明顺序键阻挡在恢复实例上仍然成立。

## 产物目录结构

```
runtime/
├── drill-summary.json                # 六次验收的 verdict 与理由汇总
└── <scenario>/
    ├── live/quaybell.sqlite3         # 事故现场在线库（受控时钟）
    ├── facts/                        # 放行方事故前留存的两份事实
    │   ├── redrive-receipt.json
    │   └── lease-fact.json
    ├── backups/                      # 两份同消息数/同最大消息号的冷备
    │   ├── backup-early.{sqlite3,manifest.json,sha256}
    │   └── backup-correct.{sqlite3,manifest.json,sha256}
    └── restored/<case>/
        ├── quaybell.sqlite3          # 恢复到全新目录的库
        ├── restore-report.json
        └── acceptance-evidence/      # 取件前依据 + 取件后比对 + 99-verdict.json
```
