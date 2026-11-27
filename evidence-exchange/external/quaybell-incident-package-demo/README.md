# Quaybell 事故证据交换包（演示）

> 本包所有数据均为虚构演示数据，不对应任何真实事故、设备或人员。

## 1. 包里有什么

| 路径 | 作用 |
| --- | --- |
| `evidence/export-1_observed-t120.trace.json` | 同一消息在观察时刻 t+120 的 `quaybell-trace/1` 脱敏证据（死信，未重投） |
| `evidence/export-2_observed-t600.trace.json` | 同一消息在观察时刻 t+600 的脱敏证据（已重投并确认） |
| `REDACTION.json` | 脱敏字段与代号规则（只写代号，不含真实名称） |
| `TIMELINE.md` | 由证据事件流自动生成的人读时间线 |
| `SENDER-ASSERTION.json` | 发送方声明：原件已内部自校验通过（签名背书） |
| `SHA256SUMS.manifest` | 包内全部可信文件的 SHA-256 清单 |
| `SHA256SUMS.sig` | 清单的 Ed25519 分离签名 |
| `signing-key-demo.pub.pem` | 验签公钥（其指纹须通过包外渠道核对） |
| `verify.py` | 接收方一键校验脚本（仅用 Python 标准库 + openssl） |
| `tamper-demo/` | **故意篡改**的教学样本，不在签名清单内，不是证据 |

两份证据的 `trace.message.id` 相同（均为 1），是同一消息在不同时间的快照；
重投关系为投递 `2` 的 `parent_id` 指向原始投递 `1`。

## 2. 接收方如何校验（建议在隔离环境运行）

```sh
# 方式一：随包脚本（标准库即可，签名验证调用 openssl）
python3 -I verify.py

# 方式二：手工核对，等价的底层命令
# (a) 单文件自摘要：弹出 sha256 字段后对规范化 JSON 重算 SHA-256
# (b) 清单哈希
sha256sum -c SHA256SUMS.manifest
# (c) 清单签名
openssl pkeyutl -verify -pubin -inkey signing-key-demo.pub.pem \
    -rawin -in SHA256SUMS.manifest -sigfile SHA256SUMS.sig
```

校验分四层：① 每份证据内嵌自摘要；② 清单文件哈希；③ 清单 Ed25519 签名；
④ 跨文件身份关联与重投关系。任一层失败都不应信任该包。

## 3. 可验证范围（重要）

接收方**能够独立验证**的，只有脱敏之后的内容：

- 两份脱敏证据自身完整、未被改动（自摘要与清单哈希）；
- 清单确实由对应私钥签名、公钥指纹经包外渠道确认无误；
- 两份证据确属同一消息、重投前后关系成立、时间戳与状态自洽；
- 载荷实值未出现在包内，仅有可跨文件比对的 `original_sha256` 摘要。

接收方**无法仅凭本包验证**的：

- 原始证据的逐字节内容、被脱敏的生产者名 / worker / 载荷实值（原件与映射表不外发）；
- “原始包校验通过”这一事实——它由 `SENDER-ASSERTION.json` 陈述并随清单签名背书，
  属于对发送方的信任，而不是接收方可独立复算的结论。

因此请区分两件事：**原始包校验通过**是发送方声明的事实；
**脱敏件可验证**的范围以上述四层为限，两者不要混为一谈。

## 4. 篡改演示怎么看

`tamper-demo/` 内的样本把导出 #1 的 `deliveries[0].state` 从 `dead` 改成了 `acked`。
运行 `verify.py` 会看到它被自摘要检查识破；它同时也与导出 #2 的事件链矛盾。
该目录刻意不纳入签名清单，切勿当作证据使用。
