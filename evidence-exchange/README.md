# 事故证据交换工作区（演示数据）

为外部设备商准备的可交换、可校验的脱敏事故证据包。所有名称、数值、时间戳均为虚构，
由真实 `quaybell` 模块在受控时钟下跑出，再按固定规则脱敏。

## 目录结构与边界

```
evidence-exchange/
├── build/                       # 构建与校验脚本（可重复运行，仅用标准库 + openssl）
│   ├── build_package.py         # 跑事故时间线 → 导出两份证据 → 脱敏 → 签名 → 篡改演示
│   ├── verify_package.py        # 接收方校验脚本（构建时复制进对外包为 verify.py）
│   └── make_delivery.py         # 打 tar.gz、干净环境复验、生成交付校验记录
├── external/                    # ★ 可对外：脱敏后的证据包（无任何内部实值）
│   └── quaybell-incident-package-demo/
├── delivery/                    # ★ 可对外：交付说明、归档、交付校验记录
│   ├── DELIVERY.md
│   ├── quaybell-incident-package-demo.tar.gz
│   └── delivery-verification-log.md
└── internal/                    # ✗ 机密，严禁外发
    ├── raw/                     # 两份原始 quaybell-trace/1 证据（含真实生产者/worker/载荷）
    ├── keys/                    # Ed25519 私钥与公钥（私钥机密，公钥随包）
    ├── redaction-mapping.CONFIDENTIAL.json  # 真实名称 ↔ 脱敏代号映射
    └── verification-record.json # 原件/脱敏件哈希链与保管链
```

## 一键复现

```sh
python3 -I evidence-exchange/build/build_package.py     # 重建 external 与 internal
python3 -I evidence-exchange/build/make_delivery.py     # 归档 + 干净环境复验 + 交付记录
```

接收方拿到包后：

```sh
tar -xzf quaybell-incident-package-demo.tar.gz
cd quaybell-incident-package-demo && python3 -I verify.py
```

## 设计要点

- **同一消息、两次导出**：两份证据 `trace.message.id` 相同、`observed_at` 不同，
  可对在一起看同一消息在死信时与重投确认后的两个快照。
- **跨文件身份关联保留**：消息/投递/尝试/事件 ID、时间戳、状态、策略版本全部不改；
  内部名称用跨文件一致的稳定代号替换。
- **重投前后关系**：重投后继投递的 `parent_id` 指向原始投递，且事件流含 `redriven`。
- **载荷只留摘要**：载荷实值移除，占位对象保留 `original_sha256`，可跨文件比对同一性。
- **四层校验**：证据自摘要 → 清单哈希 → Ed25519 签名 → 跨文件关联；
  篡改演示目录刻意排除在签名清单之外。
- **两类“通过”分离**：原件自校验通过是发送方签名背书的事实（`SENDER-ASSERTION.json`），
  接收方可独立验证的范围仅限脱敏件，二者边界在包内 README 与交付说明中明确写出。
