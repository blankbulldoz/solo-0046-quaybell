#!/usr/bin/env python3
"""对外证据包校验工具（接收方运行；仅依赖 Python 标准库，签名验证调用 openssl）。

用法：
    python3 verify.py                 # 校验脚本所在的证据包目录
    python3 verify.py /path/to/quaybell-incident-package-demo

检查分三层，输出逐项 PASS/FAIL：
  1. 每份证据文件的内嵌自摘要（sha256 字段）能否重算复现 —— 证明单文件内容未被改动；
  2. SHA256SUMS.manifest 中登记的每个文件哈希是否与磁盘一致；
  3. SHA256SUMS.sig 是否为随包公钥对清单的有效 Ed25519 签名（需要 openssl）；
  4. 跨文件身份关联与重投前后关系是否成立。

退出码：0 全部通过；1 存在失败项；2 用法或环境错误。
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path


def fail(message: str) -> None:
    print(f"  FAIL  {message}")


def passed(message: str) -> None:
    print(f"  PASS  {message}")


def info(message: str) -> None:
    print(f"  ..    {message}")


def canonical_pack(value) -> str:
    # 与 quaybell.codec.pack 同一规范化规则：排序键、无空白、ASCII 转义。
    return json.dumps(value, ensure_ascii=True, allow_nan=False,
                      sort_keys=True, separators=(",", ":"))


def fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def check_self_digest(path: Path) -> bool:
    try:
        record = json.loads(path.read_text(encoding="ascii"))
        stored = record.pop("sha256")
    except (ValueError, KeyError, OSError) as exc:
        fail(f"{path.name}: 无法解析或缺少 sha256 字段（{exc}）")
        return False
    recomputed = fingerprint(canonical_pack(record))
    if recomputed != stored:
        fail(f"{path.name}: 内嵌自摘要不匹配（重算 {recomputed[:16]}… != 记录 {stored[:16]}…）")
        return False
    passed(f"{path.name}: 内嵌自摘要重算一致（sha256={stored[:16]}…）")
    return True


def load_manifest(manifest: Path):
    entries = []
    for line in manifest.read_text(encoding="ascii").splitlines():
        if not line.strip():
            continue
        digest, relative = line.split("  ", 1)
        entries.append((digest.strip(), relative.strip()))
    return entries


def check_manifest(package_dir: Path) -> bool:
    manifest = package_dir / "SHA256SUMS.manifest"
    if not manifest.is_file():
        fail("找不到 SHA256SUMS.manifest")
        return False
    ok = True
    for digest, relative in load_manifest(manifest):
        target = package_dir / relative
        if not target.is_file():
            fail(f"清单登记的文件缺失：{relative}")
            ok = False
            continue
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        if actual != digest:
            fail(f"哈希不一致：{relative}")
            ok = False
        else:
            passed(f"清单哈希一致：{relative}")
    if ok:
        info("清单覆盖的所有文件均与登记哈希一致（注意：清单本身由签名保护）")
    return ok


def check_signature(package_dir: Path) -> bool:
    manifest = package_dir / "SHA256SUMS.manifest"
    signature = package_dir / "SHA256SUMS.sig"
    # 公钥命名以随包实际文件为准（glob 兜底，避免文件名差异导致误报）。
    candidates = sorted(package_dir.glob("*.pub.pem"))
    pub_key = candidates[0] if candidates else package_dir / "signing-key-demo.pub.pem"
    if not signature.is_file() or not pub_key.is_file():
        fail("缺少签名文件或公钥，无法验证清单签名")
        return False
    if shutil.which("openssl") is None:
        fail("未找到 openssl，跳过密码学签名验证（清单哈希仍可手工核对）")
        return False
    proc = subprocess.run(
        ["openssl", "pkeyutl", "-verify", "-pubin", "-inkey", str(pub_key),
         "-rawin", "-in", str(manifest), "-sigfile", str(signature)],
        capture_output=True,
    )
    if proc.returncode == 0 and b"Verified Successfully" in proc.stdout:
        passed(f"Ed25519 清单签名验证通过（公钥 {pub_key.name}）")
        info("公钥指纹应通过包外渠道复核："
             + hashlib.sha256(pub_key.read_bytes()).hexdigest()[:16] + "…")
        return True
    fail("清单签名验证失败：" + proc.stderr.decode(errors="replace").strip())
    return False


def check_cross_file_links(package_dir: Path) -> bool:
    evidence_dir = package_dir / "evidence"
    files = sorted(evidence_dir.glob("*.trace.json"))
    if len(files) < 2:
        fail("evidence/ 下不足两份证据，无法核对跨文件关联")
        return False
    traces = []
    for path in files:
        record = json.loads(path.read_text(encoding="ascii"))
        traces.append((path, record))

    ok = True
    first_path, first = traces[0]
    message_ids = {rec["trace"]["message"]["id"] for _, rec in traces}
    if len(message_ids) == 1:
        passed(f"两份证据指向同一消息（message_id={next(iter(message_ids))}）")
    else:
        fail(f"消息 id 不一致：{message_ids}")
        ok = False

    # 观察时刻必须不同（这是“同一消息不同时间导出”的核心）。
    observed = {rec["observed_at"] for _, rec in traces}
    if len(observed) == len(traces):
        passed(f"两份证据观察时刻不同：{sorted(observed)}")
    else:
        fail("观察时刻相同，不构成不同时间的导出")
        ok = False

    # 信封指纹与载荷摘要跨文件一致。
    fingerprints = {rec["trace"]["message"]["fingerprint"] for _, rec in traces}
    if len(fingerprints) == 1:
        passed("信封指纹跨文件一致（同一原始信封）")
    else:
        fail(f"信封指纹不一致：{fingerprints}")
        ok = False

    payload_digests = set()
    for _, rec in traces:
        payload = rec["trace"]["message"]["envelope"].get("payload", {})
        if isinstance(payload, dict) and "original_sha256" in payload:
            payload_digests.add(payload["original_sha256"])
    if payload_digests and len(payload_digests) == 1:
        passed("脱敏载荷占位摘要跨文件一致（载荷实值未出现在包内）")
    else:
        fail("载荷占位摘要缺失或不一致")
        ok = False

    # 重投前后关系：后一份证据应出现 parent_id 指向原始投递的后继投递。
    later = max(traces, key=lambda item: item[1]["observed_at"])[1]
    earlier = min(traces, key=lambda item: item[1]["observed_at"])[1]
    later_deliveries = {d["id"]: d for d in later["trace"]["deliveries"]}
    earlier_ids = {d["id"] for d in earlier["trace"]["deliveries"]}
    new_ids = set(later_deliveries) - earlier_ids
    linked = [later_deliveries[d_id] for d_id in new_ids
              if later_deliveries[d_id].get("parent_id") in earlier_ids]
    redriven_events = [e for e in later["trace"]["events"] if e["kind"] == "redriven"]
    if linked and redriven_events:
        child = linked[0]
        passed(f"重投关系成立：投递 {child['id']} 的 parent_id={child['parent_id']}，"
               f"且存在 redriven 事件")
    else:
        fail("未找到重投后继（parent_id 关联或 redriven 事件缺失）")
        ok = False

    # 脱敏一致性：同值在两份文件里必须用同一代号。
    producer_names = {rec["trace"]["message"]["producer"] for _, rec in traces}
    if len(producer_names) == 1 and "REDACTED" in next(iter(producer_names)):
        passed(f"生产者代号跨文件一致且已脱敏（{next(iter(producer_names))}）")
    else:
        fail("生产者字段脱敏代号不一致或疑似未脱敏")
        ok = False
    return ok


def check_tamper_demo(package_dir: Path) -> None:
    tamper_dir = package_dir / "tamper-demo"
    if not tamper_dir.is_dir():
        return
    print("\n[篡改演示] 该目录内容故意被篡改，且不在签名清单内：")
    for path in sorted(tamper_dir.glob("*.json")):
        if path.name == "TAMPER-README.json":
            continue
        try:
            record = json.loads(path.read_text(encoding="ascii"))
            stored = record.pop("sha256")
            recomputed = fingerprint(canonical_pack(record))
        except (ValueError, KeyError):
            info(f"{path.name}: 无法按证据格式解析")
            continue
        if recomputed != stored:
            passed(f"{path.name}: 如预期被自摘要检查识破（重算值 != 内嵌值）")
        else:
            fail(f"{path.name}: 篡改未被自摘要发现（演示异常）")


def main() -> int:
    package_dir = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent
    if not package_dir.is_dir():
        print(f"用法：{sys.argv[0]} [证据包目录]", file=sys.stderr)
        return 2

    print(f"证据包目录：{package_dir}\n")
    results = []

    print("[1/4] 单文件内嵌自摘要（脱敏件自身完整性）")
    for path in sorted((package_dir / "evidence").glob("*.trace.json")):
        results.append(check_self_digest(path))

    print("\n[2/4] 签名清单文件哈希（包级完整性）")
    results.append(check_manifest(package_dir))

    print("\n[3/4] 清单 Ed25519 签名（来源真实性）")
    results.append(check_signature(package_dir))

    print("\n[4/4] 跨文件身份关联与重投关系")
    results.append(check_cross_file_links(package_dir))

    check_tamper_demo(package_dir)

    print("\n结论：")
    if all(results):
        print("  全部检查通过。该脱敏包自身完整、签名有效、跨文件关联成立。")
        print("  可验证范围仅限脱敏后的内容；包内不含原始证据，")
        print("  “原件校验通过”是内部记录所声明的事实，无法仅凭本包复核。")
        return 0
    print("  存在失败项，请勿信任该包内容，并按交付说明联系发送方。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
