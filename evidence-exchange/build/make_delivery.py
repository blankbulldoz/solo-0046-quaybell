#!/usr/bin/env python3
"""打包对外脱敏证据并生成交付校验记录（可重复运行）。

步骤：
  1. 将 external/quaybell-incident-package-demo 打成 tar.gz（解包后顶层目录同名）；
  2. 计算归档与公钥指纹；
  3. 在干净临时目录解包，运行包内 verify.py（python3 -I），留存完整输出；
  4. 把哈希、指纹、校验结论写入 delivery/delivery-verification-log.md。

仅依赖标准库；verify.py 内部的签名验证调用 openssl。
运行：python3 -I evidence-exchange/build/make_delivery.py
"""
from __future__ import annotations

import hashlib
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
EXCHANGE = REPO_ROOT / "evidence-exchange"
PACKAGE_DIR = EXCHANGE / "external" / "quaybell-incident-package-demo"
DELIVERY_DIR = EXCHANGE / "delivery"
ARCHIVE = DELIVERY_DIR / "quaybell-incident-package-demo.tar.gz"
LOG = DELIVERY_DIR / "delivery-verification-log.md"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_archive() -> None:
    DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
    if ARCHIVE.exists():
        ARCHIVE.unlink()
    with tarfile.open(ARCHIVE, "w:gz") as tar:
        tar.add(PACKAGE_DIR, arcname=PACKAGE_DIR.name)


def verify_in_clean_dir() -> tuple[int, str]:
    with tempfile.TemporaryDirectory(prefix="quaybell-delivery-") as tmp:
        with tarfile.open(ARCHIVE, "r:gz") as tar:
            tar.extractall(tmp)
        extracted = Path(tmp) / PACKAGE_DIR.name
        proc = subprocess.run(
            [sys.executable, "-I", "verify.py"], cwd=extracted,
            capture_output=True, text=True,
        )
        return proc.returncode, proc.stdout + proc.stderr


def main() -> int:
    if not PACKAGE_DIR.is_dir():
        print("找不到对外包目录，请先运行 build_package.py", file=sys.stderr)
        return 2

    make_archive()
    archive_hash = sha256_file(ARCHIVE)
    pub_key = PACKAGE_DIR / "signing-key-demo.pub.pem"
    pub_fingerprint = sha256_file(pub_key)
    code, output = verify_in_clean_dir()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    manifest_hash = sha256_file(PACKAGE_DIR / "SHA256SUMS.manifest")
    evidence_files = sorted((PACKAGE_DIR / "evidence").glob("*.trace.json"))
    evidence_hashes = "\n".join(
        f"| `{p.name}` | `{sha256_file(p)}` |" for p in evidence_files
    )

    log = f"""# 交付校验记录（演示）

- 生成时间（UTC）：{now}
- 交付归档：`{ARCHIVE.name}`
- 归档 SHA-256：`{archive_hash}`
- 签名清单 SHA-256：`{manifest_hash}`
- 验签公钥 `{pub_key.name}` 指纹（SHA-256，需包外渠道核对）：`{pub_fingerprint}`

## 包内证据文件哈希

| 文件 | SHA-256 |
| --- | --- |
{evidence_hashes}

## 干净环境解包后运行 `python3 -I verify.py`

- 退出码：**{code}**（0 表示全部检查通过）
- 完整输出：

```text
{output.rstrip()}
```

## 校验范围声明

- 上述退出码 0 仅证明**脱敏包**自摘要、清单哈希、Ed25519 签名与跨文件关联全部成立；
- “原始证据校验通过”是发送方在 `SENDER-ASSERTION.json` 中签名背书的事实，
  原件与脱敏映射表留存于发送方 `internal/`，不在本次交付范围内；
- 本记录与归档、包内文件哈希应一并交付，接收方可重算归档哈希与此处比对。
"""
    LOG.write_text(log, encoding="utf-8")
    print(f"归档已生成：{ARCHIVE}")
    print(f"归档 SHA-256：{archive_hash}")
    print(f"校验退出码：{code}（0=通过）")
    print(f"校验记录：{LOG}")
    return 0 if code == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
