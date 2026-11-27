# 复盘账本（交接）

一次告警清理后，生产方复用了原生产键，载荷也一样。现有三份手记（05:12 已补发、
05:18 再次收到、05:21 已处理）、一张只带父子分发号的迟来重投回执，以及一份保留下来
的新消息证据。本目录把「每条声明能确认到哪一步」固定下来，并让后手能继续补证。

本目录不是中继运行时的一部分，不写入任何 Quaybell 存储。

## 结论先行

| 手记 | 能确认到 | 账本处置 |
| --- | --- | --- |
| 05:12 已补发 | S2 · 新消息身份 #2（原键同载荷，非幂等重放） | 采用，但限定「补发 = 新消息」 |
| 05:18 再次收到 | S2 · 分发 3 已领取、只有租约 | 采用，限定「收到 = 领取」 |
| 05:21 已处理 | 与保留证据相反：尝试 open，05:23:30 记 expired | **不得改写**，新消息那一行保持未处理 |

迟来重投回执导入后只把**旧链**抬到 S3（双源交叉）：1 → 2 有直接后继、2 已确认。
它不触碰新消息 #2 / 分发 3。补充材料验收结果：字段级同构成立、链路级不成立、
业务级留给人核对。

## 复现样本（可重跑）

```sh
python3 -m review.scenario --out review/evidence/run-001
```

受控时钟从 `2026-11-27T00:00:00Z` 起算，走真实 HTTP 接口和临时 SQLite；同一版代码
重复运行产出逐字节相同的材料（`diff -r` 可验证）。租约凭据每次随机且从不落入材料。

## 账本命令

```sh
python3 -m review.ledger verify                        # 核对全部引用、摘要与不变量
python3 -m review.ledger import-receipt review/supplements/receipt-1-2.json
python3 -m review.ledger accept-supplement review/supplements/supplement-mirror.json
python3 -m review.ledger show                          # 渲染成 review/LEDGER.md
```

`verify` 会做三类硬校验，任何一条不过就拒绝保存：

1. 每条引用都要在材料里逐字段对上；证据记录的 `sha256` 必须与内容自洽。
2. 只要声明动到「新消息是否已处理」，就取保留证据的尝试终态直接比对。现在终态是
   `open`，所以把它标成「已处理」一定会被拦下——换一种说法、编一条 `acked` 引用都不行。
3. 重投回执只能支撑旧链，不能出现在新消息的处置依据里。

## 当前修订

账本现为 r3：r1 标注三条手记，r2 导入迟来回执，r3 验收补充材料。逐条引用见
`review/LEDGER.md`。

## 仍未闭合（交给接手人）

- 生产侧是否把「补发」理解为同批次重放：需生产方书面确认。
- 05:21「已处理」若确有动作，须给出消息号或分发号；否则只能挂在那条链路之外。
- 补充材料要并入同一次告警，需生产侧提供 `alert-mirror` 的转发规则，由人核对后决定。

## 文件

| 路径 | 作用 |
| --- | --- |
| `review/scenario.py` | 生成同构样本（只走公共 HTTP 入口） |
| `review/ledger.py` | 核对、导入、验收、渲染账本 |
| `review/ledger.json` | 账本数据源（每条声明的台阶、判定、限定、引用） |
| `review/LEDGER.md` | 渲染后的带引用账本 |
| `review/evidence/run-001/` | 样本材料与 `MANIFEST.json` 摘要清单 |
| `review/supplements/receipt-1-2.json` | 迟来的重投回执（仅父子分发号） |
| `review/supplements/supplement-mirror.json` | 时间接近但链路不同的补充材料 |
