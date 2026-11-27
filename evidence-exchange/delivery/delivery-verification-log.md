# 交付校验记录（演示）

- 生成时间（UTC）：2026-11-27T15:58:04Z
- 交付归档：`quaybell-incident-package-demo.tar.gz`
- 归档 SHA-256：`196b95dc9f8e471f469478fb25da591b0510dc75b9161a8ed21902b4621f7d70`
- 签名清单 SHA-256：`7741c074e4301604a22c3839335938b6ec31b0d916aa57d67f9138959677ce53`
- 验签公钥 `signing-key-demo.pub.pem` 指纹（SHA-256，需包外渠道核对）：`b8c7d6e9d0371749d0f704554550764c23a51627cc8ada99e2c2314b6356436b`

## 包内证据文件哈希

| 文件 | SHA-256 |
| --- | --- |
| `export-1_observed-t120.trace.json` | `6d0648f78b4ab3e845e8fcd32acf93c764b969279e79e99fef6f5f21cf426795` |
| `export-2_observed-t600.trace.json` | `a4290943f7fc72002a833fd1bc6ab1326ce1989852ec464a07a4b816e668a9e4` |

## 干净环境解包后运行 `python3 -I verify.py`

- 退出码：**0**（0 表示全部检查通过）
- 完整输出：

```text
证据包目录：/tmp/quaybell-delivery-zws3h70w/quaybell-incident-package-demo

[1/4] 单文件内嵌自摘要（脱敏件自身完整性）
  PASS  export-1_observed-t120.trace.json: 内嵌自摘要重算一致（sha256=32da564200a46421…）
  PASS  export-2_observed-t600.trace.json: 内嵌自摘要重算一致（sha256=859bf5c1f7185880…）

[2/4] 签名清单文件哈希（包级完整性）
  PASS  清单哈希一致：README.md
  PASS  清单哈希一致：REDACTION.json
  PASS  清单哈希一致：SENDER-ASSERTION.json
  PASS  清单哈希一致：TIMELINE.md
  PASS  清单哈希一致：evidence/export-1_observed-t120.trace.json
  PASS  清单哈希一致：evidence/export-2_observed-t600.trace.json
  PASS  清单哈希一致：signing-key-demo.pub.pem
  PASS  清单哈希一致：verify.py
  ..    清单覆盖的所有文件均与登记哈希一致（注意：清单本身由签名保护）

[3/4] 清单 Ed25519 签名（来源真实性）
  PASS  Ed25519 清单签名验证通过（公钥 signing-key-demo.pub.pem）
  ..    公钥指纹应通过包外渠道复核：b8c7d6e9d0371749…

[4/4] 跨文件身份关联与重投关系
  PASS  两份证据指向同一消息（message_id=1）
  PASS  两份证据观察时刻不同：[1764000120, 1764000600]
  PASS  信封指纹跨文件一致（同一原始信封）
  PASS  脱敏载荷占位摘要跨文件一致（载荷实值未出现在包内）
  PASS  重投关系成立：投递 2 的 parent_id=1，且存在 redriven 事件
  PASS  生产者代号跨文件一致且已脱敏（PRODUCER-REDACTED-01）

[篡改演示] 该目录内容故意被篡改，且不在签名清单内：
  PASS  export-1_TAMPERED-state-flipped.trace.json: 如预期被自摘要检查识破（重算值 != 内嵌值）

结论：
  全部检查通过。该脱敏包自身完整、签名有效、跨文件关联成立。
  可验证范围仅限脱敏后的内容；包内不含原始证据，
  “原件校验通过”是内部记录所声明的事实，无法仅凭本包复核。
```

## 校验范围声明

- 上述退出码 0 仅证明**脱敏包**自摘要、清单哈希、Ed25519 签名与跨文件关联全部成立；
- “原始证据校验通过”是发送方在 `SENDER-ASSERTION.json` 中签名背书的事实，
  原件与脱敏映射表留存于发送方 `internal/`，不在本次交付范围内；
- 本记录与归档、包内文件哈希应一并交付，接收方可重算归档哈希与此处比对。
