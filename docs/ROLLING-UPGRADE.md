# 滚动升级与防误连上线手册（两端口 / 共库）

本手册对应一次真实演练，自动验收记录见
[`../ACCEPTANCE.md`](../ACCEPTANCE.md)
（机器可读：[`../acceptance/rolling-acceptance.json`](../acceptance/rolling-acceptance.json)）。
一键复跑：

```sh
python3 ops/rolling_acceptance.py
```

脚本启动真实子进程、使用真实系统时钟等待租约自然过期，约 15–25 秒，51 项检查全部通过才退出 0。

## 1. 为什么健康、频道名、条数不够

Quaybell 的状态全部落在同一个 SQLite 文件。滚动替换的正确拓扑是两个本机端口的进程
（例如 `127.0.0.1:18091` / `18092`）都打开**同一份共同库**，一次只停一个进程。

事故形态：操作员把 `--db` 指到了“从共同库复制的旧副本”。复制瞬间它与共库逐行一致——
频道名、消息条数、甚至那条尚未到期的租约（尝试号、worker、截止时间、凭据摘要）都相同。
随后共库继续推进（伙伴端写入、租约自然过期、回收、第二次尝试），旧副本却停在 fork 快照。

因此上线命令必须能拒绝两类误连：

| 误连形态 | 护栏手段 | 退出码 |
| --- | --- | --- |
| `--db` 直接指向旧副本路径 / 库文件不存在 | realpath 路径钉 `--common-db`；不存在的路径绝不隐式建空库 | `3` / `2` |
| 旧副本被复制回权威路径（路径、集群绑定都相同） | `--require-fence` 新鲜事实栅栏：fork 后伙伴端刚提交的消息、指纹、journal 水位、栅栏行必须可见 | `4` |
| 未初始化绑定的库 / 集群标识不符 | `rollout_binding` 核对 | `5` |
| 栅栏 JSON 损坏 | 读文件阶段拒绝 | `6` |

## 2. 上线命令（可直接套用）

以下用 `/srv/quaybell/common.sqlite3` 作为部署路径示例；仓库内一键验收使用
`.rollout-lab/common/quaybell.sqlite3`。

```sh
# 0) 一次性：给共同库写集群绑定（额外两张 rollout_* 旁表，不影响 quaybell schema）
python3 ops/qb_rollout.py init-cluster \
  --db /srv/quaybell/common.sqlite3 --cluster prod-a

# 1) 首代两个进程（尚无栅栏，先靠路径钉+集群绑定）
QUAYBELL_RELEASE=baseline \
python3 -m quaybell --port 18091 --db /srv/quaybell/common.sqlite3 \
  --common-db /srv/quaybell/common.sqlite3 --expect-cluster prod-a &
QUAYBELL_RELEASE=baseline \
python3 -m quaybell --port 18092 --db /srv/quaybell/common.sqlite3 \
  --common-db /srv/quaybell/common.sqlite3 --expect-cluster prod-a &

# 2) 建立栅栏专用频道（不需要消费组），再让仍在接活的 18092 提交“栅栏事实”
curl -s -X POST http://127.0.0.1:18092/v1/channels \
  -H 'Content-Type: application/json' -d '{"name":"rollout-proof"}' >/dev/null
python3 ops/qb_rollout.py fence --db /srv/quaybell/common.sqlite3 \
  --via http://127.0.0.1:18092 --channel rollout-proof \
  --out /srv/quaybell/fence-18091.json

# 3) 停掉 18091 旧进程；这期间 18092 继续发布/领取。然后带栅栏上线新进程
PORT=18091 DB=/srv/quaybell/common.sqlite3 COMMON_DB=/srv/quaybell/common.sqlite3 \
CLUSTER=prod-a FENCE=/srv/quaybell/fence-18091.json RELEASE=candidate-r053 \
  ops/go-live.sh
# 等价于：
# QUAYBELL_RELEASE=candidate-r053 python3 -m quaybell --port 18091 \
#   --db /srv/quaybell/common.sqlite3 --common-db /srv/quaybell/common.sqlite3 \
#   --expect-cluster prod-a --require-fence /srv/quaybell/fence-18091.json
# 启动前会打印 QB_GUARD_PASS {...}；不通过则 QB_GUARD_REJECT 并以 2/3/4/5/6 退出，不会监听端口。

# 4) 对 18092 重复 2)-3)，栅栏改由已换新的 18091 提交（fence-18092.json）

# 任意时刻审计任意候选库（输出完整 JSON 核对报告）
python3 ops/qb_rollout.py verify --db /path/to/candidate.sqlite3 \
  --common-db /srv/quaybell/common.sqlite3 \
  --require-fence /srv/quaybell/fence-18091.json
```

## 3. 故意保留旧库并核对“起初一致”

```sh
# SQLite backup API 一致性复制；源库处于 WAL 且有进程在写也安全
python3 ops/qb_rollout.py decoy-copy \
  --source /srv/quaybell/common.sqlite3 \
  --target /srv/quaybell/stale-copy.sqlite3

# 不戴护栏直接起一个“误连端”（仅用于验收/排障，生产禁止这样启动）
python3 -m quaybell --port 18093 --db /srv/quaybell/stale-copy.sqlite3
```

在复制 fork 的瞬间，对同一消息 `GET /v1/messages/{id}`：

* `18091`、`18092`（共库）与 `18093`（旧副本）返回相同的消息指纹；
* 同一条尝试：编号、worker、`expires`、`outcome=open` 完全一致；
* `GET /v1/channels` 的频道名/条数也一致——这正是不能只查这些项的原因。

## 4. 在手租约 + 同键后继：过期、回收与判别

验收构造：频道 `rollout-lease` / 组 `gears` / 策略
`lease_seconds=8, retry_seconds=0, max_attempts=3`；两条消息 M1、M2 使用**同一顺序键**
（`lane=same-order-key`），生产键不同。

1. 8 个并发 claim 一半打 A、一半打 B（线程栅栏同时发出）。
   只有一个响应领到 M1 的尝试 #1；M2 作为同键后继始终被队首挡住，任何响应都领不到。
   数据库层面任意时刻 `attempts` 对该投递只有一行 `outcome='open'`
   （部分唯一索引 `one_open_attempt`），写事务 `BEGIN IMMEDIATE` 把并发领取串行化。
2. 此时做旧库复制。
3. 不确认、不拒绝、不暂停，等待真实时钟越过租约 `expires`。回收前只读
   `GET /v1/messages/M1` 会看到尝试 #1 仍是 `open`，但 `now > expires`
   ——即“事实上已自然过期、状态尚未推进”。
4. 请**未持有该租约的另一端** `POST /v1/sweep`（全局有界回收）。返回中 M1 为
   `{attempt:1, outcome:'expired', state:'waiting', reason:'租约超时'}`。
5. 再次 8 并发跨端口 claim：恰好一个响应得到尝试 #2（新编号、新 token 摘要、新截止），
   M2 仍不发放，open 尝试仍只有一行。

### 如何分清“共库已推进”与“启动到了旧副本”

截止时间本身不是证据：旧副本里的 `expires` 一样会随墙上时间过去。要同时核对：

| 核对点 | 真的共库、状态已推进 | 启动到了旧副本 |
| --- | --- | --- |
| 启动栅栏 `--require-fence` | `QB_GUARD_PASS` | `QB_GUARD_REJECT / STALE_REPLICA`（exit 4） |
| fork 后那条伙伴提交的消息 | 替换端 `GET /v1/messages/{fence_id}` = 200，指纹一致 | `404 not_found` |
| journal 水位 | `MAX(journal.id) >= 栅栏水位`，有该消息 `published` 事件 | 水位停在 fork 点，无栅栏行 |
| M1 尝试序列 | `#1 expired → #2 open`（两端 HTTP 追溯逐字段一致） | 自己的回收器运行前一直是 `#1 open`，无 `expired`/`#2` |
| 旧副本若也被 sweep+claim（阴性实验） | 共库 #2 凭据摘要唯一 | 旧副本发出自己的 #2，token 摘要与共库不同＝脑裂，会为同键向两个 worker 重复发租约 |

验收脚本会真的对误连端 `18093` 做最后那一记 sweep+claim 作为**受控阴性实验**
（只作用于诱饵库），证明脑裂后果，随后关闭该进程。两份旧库实物保留在
[`acceptance/evidence/`](../acceptance/evidence/)：

* `pristine-fork-copy.sqlite3`：fork 瞬间未改动的旧库（仅 `#1/open`，2 条消息）；
* `stale-copy-brain-split.sqlite3`：阴性实验后自行推进的旧库（`#1/expired` + 自己的 `#2/open`）。

可用以下命令独立复核：

```sh
python3 - <<'PY'
import sqlite3
for p in ('acceptance/evidence/pristine-fork-copy.sqlite3',
          'acceptance/evidence/stale-copy-brain-split.sqlite3'):
    db = sqlite3.connect(f'file:{p}?mode=ro', uri=True)
    print(p, db.execute(
        'SELECT delivery_id,number,outcome,worker FROM attempts ORDER BY id').fetchall())
PY
```

## 5. 放行判据（全部满足）

1. 一次只停一个端口；停 A 期间 B、停 B 期间 A1 的发布与领取都有成功实录。
2. 替换端启动日志有 `QB_GUARD_PASS`，且经 HTTP 读到伙伴端在替换前刚提交的栅栏消息，
   指纹一致；替换端新写入的消息也能被未重启/后替换的一端读到。
3. 两轮跨端口 8 并发 claim 都只有一个 M1 赢家、M2 发放数为 0、open 尝试行数为 1。
4. 原租约被观察到越过真实截止仍为 `#1/open`，由另一端 sweep 成 `expired`，
   随后唯一地产生 `#2`。
5. 诱饵旧库起初三端视图一致，之后冻结在 `#1/open`、栅栏消息 404；
   上线命令对错误路径实录 exit 3、对“放回权威路径的旧副本”实录 exit 4。
6. 两个端口最终都返回新的 `X-Quaybell-Release` 代号与完全一致的追溯视图。

## 6. 安全边界

验收脚本只调用 `channels`/`groups`/`messages`/`claim`/`sweep` 与只读 `GET`；
**不调用** pause、ack、nack、redrive、retention 清理，不删除任何消息。
连续性探针消息的领取不确认，租约自然到期；唯一的“处置”是题目要求的 sweep 回收，
它是系统内置、可重入且不删业务消息的动作。所有验收数据在独立的 `.rollout-lab/`
与 `acceptance/evidence/` 中，不碰默认 `.quaybell/`。
