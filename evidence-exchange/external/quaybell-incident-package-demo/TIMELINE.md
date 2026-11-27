# 事故时间线（演示数据）

本文件所有名称、数值与时间戳均为虚构，仅用于说明证据包的读法。
受控时钟基准 `t = 1764000000`（Unix 秒）。下表时间与事实均由两份证据的事件流自动生成。

| 相对时间 | 事件 | 关键事实 |
| --- | --- | --- |
| t+0 | 消息发布 | message_id=1，策略 v1，生成原始投递 1 |
| t+5 | 第 1 次领取 | WORKER-REDACTED-A 领取投递 1，attempt #1，租约截止 t+35 |
| t+40 | 租约超时回收 | 投递 1 attempt #1 outcome=expired，退避后就绪 t+45 |
| t+50 | 第 2 次领取 | WORKER-REDACTED-B 领取投递 1，attempt #2，租约截止 t+80 |
| t+50 | 明确拒绝 | 投递 1 attempt #2 outcome=nacked，退避后就绪 t+70，状态回到 waiting |
| t+70 | 第 3 次领取 | WORKER-REDACTED-A 领取投递 1，attempt #3，租约截止 t+100 |
| t+70 | 明确拒绝 | 投递 1 attempt #3 outcome=nacked，尝试次数用尽，投递进入 dead（不再调度） |
| t+120 | **证据导出 #1** | observed_at=1764000120，死信状态，尚未重投 |
| t+180 | 策略修订 v2 后重投 | 重投生成后继投递 2，`2.parent_id = 1`，绑定策略 v2 |
| t+190 | 第 1 次领取 | WORKER-REDACTED-B 领取投递 2，attempt #1，租约截止 t+205 |
| t+190 | 重投投递确认 | 投递 2 attempt #1 outcome=acked，状态 acked |
| t+600 | **证据导出 #2** | observed_at=1764000600，重投已确认；同一 message_id=1 |

## 两份证据如何对在一起看

1. 两份文件的 `trace.message.id` 相同（均为 1），证明是同一消息在不同观察时刻的快照。
2. 导出 #1 只含原始投递 1（state=dead）；导出 #2 在此基础上多出重投后继 2，
   且 `2.parent_id = 1`，事件流中出现 `redriven` 事件——这就是重投前后关系。
3. 两份文件中 envelope 指纹（`trace.message.fingerprint`）一致，载荷占位对象中的
   `original_sha256` 也一致，可核对“同一载荷”，但看不到载荷实值。
4. 生产者、worker、生产键、顺序键等内部名称均为跨文件一致的脱敏代号。

## 摘要层次（各自的可验证范围不同）

- 原始内部证据自摘要（仅内部留存，不在本包内）：
  export#1=`7d920bf0c4aa17c0aa0fa881333c1ca5040f6701c822c020cfec7fe125ce8d30`
  export#2=`534fd2777ce0e12dfe85f2baa6f1a9577e542094adf3b6eefb65825ec15ba981`
- 本包内脱敏件自摘要（文件内 `sha256` 字段，接收方可自行重算核对）：
  export#1=`32da564200a4642107f39099d2815fd512d44f3ddbb35f2a6390d703a049697c`
  export#2=`859bf5c1f7185880bc13550fd3946b4f2676f2590aac5b5d4b0469267997b38c`
- 两组摘要不同是预期结果：脱敏改变了字节内容。脱敏件只能自证其脱敏后内容未被改动，
  不能自证与原件逐字节相同；原件与脱敏件之间的对应关系由内部校验记录与签名清单背书。
