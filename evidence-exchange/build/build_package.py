#!/usr/bin/env python3
"""构建对外可交换的脱敏事故证据包（全部使用演示数据）。

流程：
  1. 用真实 quaybell 模块 + 内存式临时 SQLite + 受控时钟，跑完一条
     发布 -> 领取 -> 超时/拒绝 -> 死信 -> 重投 -> 确认 的事故时间线。
  2. 在两个观察时刻各导出一份真实 quaybell-trace/1 证据（同一消息、不同时间）。
  3. 原件（含内部生产者名/worker/载荷实值）只写入 internal/，不对外。
  4. 按固定脱敏表生成脱敏件，保持跨文件身份关联与重投前后关系。
  5. 为脱敏件生成 SHA-256 清单与 Ed25519 分离签名，并做一次篡改演示。

本脚本只依赖 Python 标准库；签名/验签的密码学操作通过 openssl CLI 完成。
运行方式：python3 -I build/build_package.py
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# 让脚本可以从仓库内直接 import quaybell（脚本以 -I 运行，不依赖当前工作目录）。
# -I 隔离模式下 __file__ 可能是相对路径，统一转绝对路径后再取仓库根。
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from quaybell import (  # noqa: E402  (import 需在 sys.path 配置之后)
    attempts, catalog, claims, delivery, evidence, expiry, outcomes,
    publication, redrive, subscriptions,
)
from quaybell.codec import fingerprint, pack, unpack  # noqa: E402
from quaybell.persistence import RelayPersistence  # noqa: E402

# ---------------------------------------------------------------------------
# 固定演示坐标：所有数据均为虚构，时间戳为受控时钟，不对应任何真实事故。
# ---------------------------------------------------------------------------
BASE_TS = 1_764_000_000  # 2025-11-24T13:20:00Z 附近的演示基准时间
CHANNEL = "edge-relay-demo"          # 演示频道名（虚构）
GROUP = "field-collector-demo"       # 演示消费组名（虚构）
WORKER_A = "ingest-worker-07"        # 内部 worker 名（将被脱敏）
WORKER_B = "ingest-worker-12"        # 内部 worker 名（将被脱敏）
PRODUCER = "plant-sensor-gateway-03"  # 内部生产者名（将被脱敏）
DEDUP_KEY = "frame-batch/2025-11-24/0042"  # 内部生产键（将被脱敏）
LANE = "conveyor-line-2"             # 内部顺序键（将被脱敏）
PAYLOAD = {                          # 内部载荷实值（将被脱敏，仅保留摘要）
    "batch_id": "BATCH-7731",
    "temperature_c": 84.7,
    "vibration_mm_s": 12.9,
    "status": "over-threshold",
    "operator_note": "line 2 bearing housing hot to touch",
}
TAGS = {"feed": "telemetry", "zone": "plant-b"}

# 对外脱敏替换表（跨文件一致；真实映射仅保存在 internal/）。
REDACTION = {
    PRODUCER: "PRODUCER-REDACTED-01",
    WORKER_A: "WORKER-REDACTED-A",
    WORKER_B: "WORKER-REDACTED-B",
    DEDUP_KEY: "PROD-KEY-REDACTED-01",
    LANE: "LANE-REDACTED-01",
}
# envelope 内的标签键值、频道/组名同样属于内部拓扑，一并脱敏。
REDACTION.update({
    "telemetry": "TAGVAL-REDACTED-01",
    "plant-b": "TAGVAL-REDACTED-02",
    CHANNEL: "CHANNEL-REDACTED-01",
    GROUP: "GROUP-REDACTED-01",
})

EXTERNAL_NAME = "quaybell-incident-package-demo"
EXTERNAL_DIR = REPO_ROOT / "evidence-exchange" / "external" / EXTERNAL_NAME
INTERNAL_DIR = REPO_ROOT / "evidence-exchange" / "internal"
DELIVERY_DIR = REPO_ROOT / "evidence-exchange" / "delivery"
KEYS_DIR = INTERNAL_DIR / "keys"
RAW_DIR = INTERNAL_DIR / "raw"

# 两个证据导出观察时刻（见 README 时间线说明）。
OBSERVE_1 = BASE_TS + 120   # 第一次导出：死信刚形成
OBSERVE_2 = BASE_TS + 600   # 第二次导出：重投并确认之后


def run(cmd: list[str], *, input_bytes: bytes | None = None) -> bytes:
    proc = subprocess.run(cmd, input=input_bytes, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"命令失败 {cmd[0]}: {proc.stderr.decode(errors='replace')}")
    return proc.stdout


def openssl(*args: str, input_bytes: bytes | None = None) -> bytes:
    # OpenSSL 3 的 pkeyutl -rawin 不接受纯管道输入，需要显式 -in 文件。
    if input_bytes is not None and "-rawin" in args:
        import tempfile as _tmp
        with _tmp.NamedTemporaryFile() as handle:
            handle.write(input_bytes)
            handle.flush()
            # OpenSSL 3 的 pkeyutl -rawin 需要显式 -in 文件，不能走标准输入。
            cmd: list[str] = []
            for arg in args:
                cmd.append(arg)
                if arg == "-rawin":
                    cmd.extend(["-in", handle.name])
            return run(["openssl", *cmd])
    return run(["openssl", *args], input_bytes=input_bytes)


# ---------------------------------------------------------------------------
# 第一步：用真实模块跑事故时间线
# ---------------------------------------------------------------------------
def build_live_trace(workdir: Path):
    db_path = workdir / "demo.sqlite3"
    persistence = RelayPersistence(db_path)
    clock = {"now": float(BASE_TS)}

    def tick(seconds: int):
        clock["now"] += seconds

    with persistence.session(write=True) as db:
        # 策略 v1：租约 30s、初始退避 10s、最多 3 次、保留 7200s。
        policy_v1 = dict(lease_seconds=30, retry_seconds=10,
                         max_attempts=3, retention_seconds=7200)
        catalog.create(db, {"name": CHANNEL, "policy": policy_v1}, clock["now"])
        subscriptions.create(db, CHANNEL, {"name": GROUP, "tags": {}}, clock["now"])

    envelope = {
        "producer": PRODUCER, "key": DEDUP_KEY, "lane": LANE,
        "tags": TAGS, "payload": PAYLOAD,
    }

    with persistence.session(write=True) as db:
        # t = BASE_TS：发布，生成原始投递 D1。
        receipt = publication.publish(db, CHANNEL, envelope, clock["now"])
        message_id = receipt["message_id"]
        d1 = receipt["fanout"][GROUP]

    # ---- 第一次尝试：worker A 领取，租约超时 ----
    tick(5)  # now = t+5；租约 30s，到期时刻 t+35
    with persistence.session(write=True) as db:
        claim1 = claims.claim(db, CHANNEL, GROUP,
                              {"worker": WORKER_A, "limit": 10}, clock["now"])
        lease1 = claim1["items"][0]["lease"]["token"]
        assert claim1["items"][0]["delivery_id"] == d1
    # tick 35 → now=t+40，租约早已到期；回收后 attempt #1 expired，
# 超时退避从租约截止时间起算：ready_at = t+35 + 10 = t+45。
    tick(35)
    with persistence.session(write=True) as db:
        recovered = expiry.reap(db, clock["now"], 100)
        assert recovered and recovered[0]["delivery_id"] == d1

    # ---- 第二次尝试：worker B 领取，明确拒绝 ----
    tick(10)  # now = t+50（投递 t+45 已就绪）
    with persistence.session(write=True) as db:
        claim2 = claims.claim(db, CHANNEL, GROUP,
                              {"worker": WORKER_B, "limit": 10}, clock["now"])
        lease2 = claim2["items"][0]["lease"]["token"]
        # 明确拒绝，退避从处置时间起算 10*2^(2-1)=20s → ready_at=t+70。
        outcome = outcomes.apply(
            db, delivery.get(db, d1), attempts.by_token(db, d1, lease2),
            "nacked", "下游校验帧不匹配（演示原因）", clock["now"],
        )
        assert outcome["state"] == "waiting"

    # ---- 第三次尝试：worker A 领取，再次拒绝，用尽次数进入死信 ----
    tick(20)  # now = t+70，恰好就绪
    with persistence.session(write=True) as db:
        claim3 = claims.claim(db, CHANNEL, GROUP,
                              {"worker": WORKER_A, "limit": 10}, clock["now"])
        lease3 = claim3["items"][0]["lease"]["token"]
        outcomes.apply(
            db, delivery.get(db, d1), attempts.by_token(db, d1, lease3),
            "nacked", "重试后校验帧仍不匹配（演示原因）", clock["now"],
        )
        assert delivery.get(db, d1)["state"] == "dead"

    # ---- 观察时刻 1：导出第一份证据（死信状态，尚未重投）----
    tick(20)  # now = t+90；导出对齐到观察时刻 t+120
    clock["now"] = float(OBSERVE_1)
    with persistence.session() as db:
        raw1 = evidence.export_trace(db, message_id, OBSERVE_1)

    # ---- 策略修订到 v2（缩短租约、降低退避），随后重投 ----
    tick(60)  # now = OBSERVE_1 + 60 = t+180
    with persistence.session(write=True) as db:
        catalog.update_policy(
            db, CHANNEL,
            {"lease_seconds": 15, "retry_seconds": 5, "max_attempts": 3,
             "retention_seconds": 7200, "expected": 1}, clock["now"],
        )
        redriven = redrive.redrive(db, d1, clock["now"])
        d2 = redriven["delivery_id"]
        assert redriven["policy"] == 2

    # ---- 重投后继 D2 由 worker B 领取并确认 ----
    tick(10)  # now = t+190
    with persistence.session(write=True) as db:
        claim4 = claims.claim(db, CHANNEL, GROUP,
                              {"worker": WORKER_B, "limit": 10}, clock["now"])
        item4 = claim4["items"][0]
        assert item4["delivery_id"] == d2 and item4["policy"] == 2
        outcomes.apply(
            db, delivery.get(db, d2),
            attempts.by_token(db, d2, item4["lease"]["token"]),
            "acked", None, clock["now"],
        )
        assert delivery.get(db, d2)["state"] == "acked"

    # ---- 观察时刻 2：导出第二份证据（重投已确认）----
    clock["now"] = float(OBSERVE_2)
    with persistence.session() as db:
        raw2 = evidence.export_trace(db, message_id, OBSERVE_2)

    return message_id, d1, d2, raw1, raw2


# ---------------------------------------------------------------------------
# 第二步：脱敏（递归、确定性字符串替换；载荷整体用占位对象替换并保留摘要）
# ---------------------------------------------------------------------------
PAYLOAD_DIGEST = hashlib.sha256(pack(PAYLOAD).encode("utf-8")).hexdigest()
PAYLOAD_PLACEHOLDER = {
    "_redacted": True,
    "reason": "payload-value-internal",
    "original_sha256": PAYLOAD_DIGEST,
}


def redact_value(node):
    if isinstance(node, str):
        return REDACTION.get(node, node)
    if isinstance(node, list):
        return [redact_value(item) for item in node]
    if isinstance(node, dict):
        result = {}
        for key, value in node.items():
            # 键也可能承载内部名（如 fanout 以消费组名为键），需同步脱敏。
            safe_key = REDACTION.get(key, key) if isinstance(key, str) else key
            if key == "payload":
                result[safe_key] = PAYLOAD_PLACEHOLDER
            else:
                result[safe_key] = redact_value(value)
        return result
    return node


def redact_trace(raw_bytes: bytes):
    record = unpack(raw_bytes.decode("ascii"))
    original_self_hash = record["sha256"]
    record.pop("sha256")
    # 脱敏后重算自摘要：脱敏件自洽，但与原件摘要不同（这是预期且可解释的）。
    sanitized = redact_value(record)
    sanitized["sha256"] = fingerprint(pack(sanitized))
    return sanitized, original_self_hash


# ---------------------------------------------------------------------------
# 第三步：清单与签名
# ---------------------------------------------------------------------------
def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(package_dir: Path, manifest_name: str = "SHA256SUMS.manifest"):
    # 清单显式排除：清单/签名自身、篡改演示目录（其存在意义就是“不在信任范围内”）。
    excluded_names = {manifest_name, "SHA256SUMS.sig"}
    excluded_dirs = {"tamper-demo"}
    files = sorted(
        p for p in package_dir.rglob("*")
        if p.is_file()
        and p.name not in excluded_names
        and not (set(p.relative_to(package_dir).parts) & excluded_dirs)
    )
    lines = []
    for path in files:
        lines.append(f"{sha256_file(path)}  {path.relative_to(package_dir).as_posix()}")
    manifest = package_dir / manifest_name
    manifest.write_text("\n".join(lines) + "\n", encoding="ascii")
    return manifest


def main():
    # 清理旧产物，保证可重复构建。
    for target in (EXTERNAL_DIR, RAW_DIR, KEYS_DIR):
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="quaybell-evidence-") as tmp:
        message_id, d1, d2, raw1, raw2 = build_live_trace(Path(tmp))

    # --- 内部留存：原始证据（不外发）---
    raw1_path = RAW_DIR / "raw-export-1_trace.json"
    raw2_path = RAW_DIR / "raw-export-2_trace.json"
    raw1_path.write_bytes(raw1)
    raw2_path.write_bytes(raw2)

    sanitized1, orig_hash1 = redact_trace(raw1)
    sanitized2, orig_hash2 = redact_trace(raw2)

    # 组装对外包。
    package_dir = EXTERNAL_DIR
    (package_dir / "evidence").mkdir(parents=True)
    ev1 = package_dir / "evidence" / "export-1_observed-t120.trace.json"
    ev2 = package_dir / "evidence" / "export-2_observed-t600.trace.json"
    ev1.write_text(pack(sanitized1) + "\n", encoding="ascii")
    ev2.write_text(pack(sanitized2) + "\n", encoding="ascii")

    # 脱敏说明（本身也纳入签名清单）。注意：本文件只描述“字段→代号”的规则，
    # 绝不回显真实名称；真实值到代号的映射仅保存在 internal 机密文件中。
    redaction_note = {
        "package": EXTERNAL_NAME,
        "schema": "quaybell-trace/1 (sanitized copy)",
        "demo_only": True,
        "redaction_date": "2025-11-25",
        "rules": [
            {"field": "trace.message.producer", "action": "pseudonym",
             "replacement": REDACTION[PRODUCER]},
            {"field": "trace.message.dedup_key", "action": "pseudonym",
             "replacement": REDACTION[DEDUP_KEY]},
            {"field": "trace.message.lane and *.lane", "action": "pseudonym",
             "replacement": REDACTION[LANE]},
            {"field": "*.envelope.payload", "action": "removed",
             "replacement": "placeholder carrying original_sha256 only"},
            {"field": "*.worker", "action": "pseudonym",
             "replacements": ["WORKER-REDACTED-A", "WORKER-REDACTED-B"],
             "note": "两个内部 worker 的稳定代号；真实名称到代号的映射留存于发送方内部，不在本包内"},
            {"field": "channel / group / tag values (including object keys such as fanout)",
             "action": "pseudonym",
             "note": "内部拓扑名称（含作为对象键的组名）按固定代号一致替换"},
        ],
        "preserved": [
            "message id and delivery/attempt/event ids (stable across both files)",
            "parent_id redelivery linkage (delivery D2.parent_id == D1.id)",
            "all timestamps, states, outcomes, policy versions and attempt counts",
            "envelope fingerprint (sha256 of the original canonical envelope)",
            "payload digest in the placeholder, so equality across files is checkable",
        ],
        "identity_link": {
            "message_id": message_id,
            "same_message_in_both_exports": True,
            "original_delivery_id": d1,
            "redriven_delivery_id": d2,
            "redelivery_relation": f"delivery {d2} is the redriven successor of {d1}",
        },
    }
    (package_dir / "REDACTION.json").write_text(
        json.dumps(redaction_note, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    # 时间线说明（人读，随包签名）。从脱敏证据的真实事件流动态生成，避免硬编码漂移。
    timeline = build_timeline_doc(sanitized1, sanitized2,
                                  orig_hash1, orig_hash2, d1, d2)
    (package_dir / "TIMELINE.md").write_text(timeline, encoding="utf-8")

    # --- 签名密钥（演示用，放 internal/；公钥进包）---
    priv_key = KEYS_DIR / "demo-ed25519.pem"
    pub_key = KEYS_DIR / "demo-ed25519.pub.pem"
    openssl("genpkey", "-algorithm", "Ed25519", "-out", str(priv_key))
    openssl("pkey", "-in", str(priv_key), "-pubout", "-out", str(pub_key))
    shutil.copy2(pub_key, package_dir / "signing-key-demo.pub.pem")

    # 校验脚本与交付说明随包分发，并纳入签名清单（脚本完整性因此受签名保护，
    # 信任根是经包外渠道核对的公钥，而非脚本自证）。
    shutil.copy2(REPO_ROOT / "evidence-exchange" / "build" / "verify_package.py",
                 package_dir / "verify.py")
    (package_dir / "README.md").write_text(
        build_readme(sanitized1, sanitized2, message_id, d1, d2), encoding="utf-8")
    (package_dir / "SENDER-ASSERTION.json").write_text(
        json.dumps(build_sender_assertion(message_id, d1, d2, orig_hash1, orig_hash2,
                                          sanitized1, sanitized2),
                   ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")

    # --- 清单 + 签名：覆盖除清单/签名自身与 tamper-demo 外的全部入包文件 ---
    manifest = write_manifest(package_dir)
    sig_path = package_dir / "SHA256SUMS.sig"
    signature = openssl("pkeyutl", "-sign", "-inkey", str(priv_key),
                        "-rawin", input_bytes=manifest.read_bytes())
    sig_path.write_bytes(signature)

    # --- 篡改演示：独立目录、显式排除在签名清单之外，展示改一项如何被发现 ---
    tamper_dir = package_dir / "tamper-demo"
    tamper_dir.mkdir()
    tampered = json.loads(ev1.read_text(encoding="ascii"))
    original_state = tampered["trace"]["deliveries"][0]["state"]
    tampered["trace"]["deliveries"][0]["state"] = "acked"  # 把 dead 改成 acked
    (tamper_dir / "export-1_TAMPERED-state-flipped.trace.json").write_text(
        pack(tampered) + "\n", encoding="ascii")
    tamper_note = {
        "demo_only": True,
        "base_file": "evidence/export-1_observed-t120.trace.json",
        "mutation": "trace.deliveries[0].state: 'dead' -> 'acked'",
        "original_value": original_state,
        "how_detected": [
            "文件字节变化 -> 与 SHA256SUMS.manifest 中记录的哈希不一致",
            "重新核对自摘要 -> 重算的 sha256 与文件内 sha256 字段不一致",
            "跨文件矛盾 -> 与 export-2 中同一 delivery 的状态/事件链对不上",
        ],
        "note": "本目录文件故意被篡改，且不纳入签名清单，仅用于教学演示，不属于可信证据。",
    }
    (tamper_dir / "TAMPER-README.json").write_text(
        json.dumps(tamper_note, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")

    # --- 内部校验记录：原件自摘要核对 + 原件/脱敏件哈希链 ---
    internal_record = build_internal_record(
        raw1_path, raw2_path, ev1, ev2, orig_hash1, orig_hash2,
        sanitized1, sanitized2, message_id, d1, d2, manifest, pub_key,
    )
    (INTERNAL_DIR / "verification-record.json").write_text(
        json.dumps(internal_record, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")

    # 内部脱敏映射（机密，不外发）。
    mapping = {
        "confidential": True,
        "do_not_distribute": True,
        "pseudonym_map": REDACTION,
        "payload": {"original": PAYLOAD, "payload_sha256": PAYLOAD_DIGEST},
    }
    (INTERNAL_DIR / "redaction-mapping.CONFIDENTIAL.json").write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")

    print("构建完成：")
    print(f"  对外包目录: {package_dir}")
    print(f"  内部留存:   {INTERNAL_DIR}")
    print(f"  消息 id={message_id} 原始投递={d1} 重投后继={d2}")


def _rel(ts: float) -> str:
    """把绝对 Unix 秒格式化成相对基准 t 的 t+N。"""
    delta = int(round(ts - BASE_TS))
    return f"t+{delta}"


def build_timeline_doc(sanitized1, sanitized2, h1, h2, d1, d2) -> str:
    """从两份脱敏证据的真实事件流构建时间线，保证文档与数据一致。"""
    message_id = sanitized2["trace"]["message"]["id"]
    later_events = sanitized2["trace"]["events"]
    deliveries = {d["id"]: d for d in sanitized2["trace"]["deliveries"]}

    # 事件 -> 人类可读描述（全部取自证据实值，不硬编码时刻）。
    rows: list[tuple[float, str, str]] = []
    for event in later_events:
        at, kind, det = event["at"], event["kind"], event["details"]
        if kind == "published":
            rows.append((at, "消息发布",
                         f"message_id={message_id}，策略 v{det['policy']}，生成原始投递 {d1}"))
        elif kind == "leased":
            rows.append((at, f"第 {det['attempt']} 次领取",
                         f"{det['worker']} 领取投递 {det['delivery_id']}，attempt #{det['attempt']}，"
                         f"租约截止 {_rel(det['expires'])}"))
        elif kind == "expired":
            rows.append((at, "租约超时回收",
                         f"投递 {det['delivery_id']} attempt #{det['attempt']} outcome=expired，"
                         f"退避后就绪 {_rel(det['effective_at'])}"))
        elif kind == "nacked":
            if det["state"] == "dead":
                fact = (f"投递 {det['delivery_id']} attempt #{det['attempt']} outcome=nacked，"
                        "尝试次数用尽，投递进入 dead（不再调度）")
            else:
                fact = (f"投递 {det['delivery_id']} attempt #{det['attempt']} outcome=nacked，"
                        f"退避后就绪 {_rel(det['effective_at'])}，状态回到 {det['state']}")
            rows.append((at, "明确拒绝", fact))
        elif kind == "redriven":
            rows.append((at, "策略修订 v2 后重投",
                         f"重投生成后继投递 {det['delivery_id']}，"
                         f"`{det['delivery_id']}.parent_id = {det['source_id']}`，绑定策略 v{det['policy']}"))
        elif kind == "acked":
            rows.append((at, "重投投递确认",
                         f"投递 {det['delivery_id']} attempt #{det['attempt']} outcome=acked，状态 {det['state']}"))

    # 在对应位置插入两次证据导出观察点。
    rows.append((sanitized1["observed_at"], "**证据导出 #1**",
                 f"observed_at={sanitized1['observed_at']}，死信状态，尚未重投"))
    rows.append((sanitized2["observed_at"], "**证据导出 #2**",
                 f"observed_at={sanitized2['observed_at']}，重投已确认；同一 message_id={message_id}"))
    rows.sort(key=lambda item: item[0])

    table = "\n".join(f"| {_rel(at)} | {label} | {fact} |" for at, label, fact in rows)

    return f"""# 事故时间线（演示数据）

本文件所有名称、数值与时间戳均为虚构，仅用于说明证据包的读法。
受控时钟基准 `t = {BASE_TS}`（Unix 秒）。下表时间与事实均由两份证据的事件流自动生成。

| 相对时间 | 事件 | 关键事实 |
| --- | --- | --- |
{table}

## 两份证据如何对在一起看

1. 两份文件的 `trace.message.id` 相同（均为 {message_id}），证明是同一消息在不同观察时刻的快照。
2. 导出 #1 只含原始投递 {d1}（state={deliveries[d1]['state']}）；导出 #2 在此基础上多出重投后继 {d2}，
   且 `{d2}.parent_id = {d1}`，事件流中出现 `redriven` 事件——这就是重投前后关系。
3. 两份文件中 envelope 指纹（`trace.message.fingerprint`）一致，载荷占位对象中的
   `original_sha256` 也一致，可核对“同一载荷”，但看不到载荷实值。
4. 生产者、worker、生产键、顺序键等内部名称均为跨文件一致的脱敏代号。

## 摘要层次（各自的可验证范围不同）

- 原始内部证据自摘要（仅内部留存，不在本包内）：
  export#1=`{h1}`
  export#2=`{h2}`
- 本包内脱敏件自摘要（文件内 `sha256` 字段，接收方可自行重算核对）：
  export#1=`{sanitized1['sha256']}`
  export#2=`{sanitized2['sha256']}`
- 两组摘要不同是预期结果：脱敏改变了字节内容。脱敏件只能自证其脱敏后内容未被改动，
  不能自证与原件逐字节相同；原件与脱敏件之间的对应关系由内部校验记录与签名清单背书。
"""


def build_sender_assertion(message_id, d1, d2, h1, h2, sanitized1, sanitized2) -> dict:
    """发送方对外声明：声明原件已自校验通过，但不暴露任何内部实值。

    接收方无法仅凭脱敏包复核“原件校验通过”，该事实由此签名声明（随清单一起被签名）背书。
    """
    return {
        "demo_only": True,
        "statement": (
            "发送方声明：与本包两份脱敏证据对应的两份原始 quaybell-trace/1 证据，"
            "在脱敏前均已通过内嵌自摘要（sha256 字段）重算核对；"
            "脱敏按固定替换表执行，未改动任何身份标识、时间戳、状态或关联关系。"
        ),
        "original_exports_verified_internally": [
            {"export": 1, "observed_at": sanitized1["observed_at"],
             "original_self_digest_sha256": h1},
            {"export": 2, "observed_at": sanitized2["observed_at"],
             "original_self_digest_sha256": h2},
        ],
        "sanitized_self_digests": [
            {"export": 1, "sanitized_self_digest_sha256": sanitized1["sha256"]},
            {"export": 2, "sanitized_self_digest_sha256": sanitized2["sha256"]},
        ],
        "identity": {
            "message_id": message_id,
            "original_delivery_id": d1,
            "redriven_delivery_id": d2,
            "relation": f"delivery {d2}.parent_id == {d1}",
        },
        "verifiability_boundary": [
            "接收方可独立验证：脱敏件自摘要、清单哈希、清单 Ed25519 签名、跨文件关联。",
            "接收方无法仅凭本包验证：原始证据的字节内容与内部名称实值（原件不外发）。",
            "“原始包校验通过”是发送方在本声明中陈述、并以同一私钥签名背书的事实。",
        ],
    }


def build_readme(sanitized1, sanitized2, message_id, d1, d2) -> str:
    return f"""# Quaybell 事故证据交换包（演示）

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

两份证据的 `trace.message.id` 相同（均为 {message_id}），是同一消息在不同时间的快照；
重投关系为投递 `{d2}` 的 `parent_id` 指向原始投递 `{d1}`。

## 2. 接收方如何校验（建议在隔离环境运行）

```sh
# 方式一：随包脚本（标准库即可，签名验证调用 openssl）
python3 -I verify.py

# 方式二：手工核对，等价的底层命令
# (a) 单文件自摘要：弹出 sha256 字段后对规范化 JSON 重算 SHA-256
# (b) 清单哈希
sha256sum -c SHA256SUMS.manifest
# (c) 清单签名
openssl pkeyutl -verify -pubin -inkey signing-key-demo.pub.pem \\
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
"""


def build_internal_record(raw1_path, raw2_path, ev1, ev2, orig_hash1, orig_hash2,
                          sanitized1, sanitized2, message_id, d1, d2,
                          manifest, pub_key) -> dict:
    def self_check(raw_path: Path, expected: str):
        record = unpack(raw_path.read_text(encoding="ascii"))
        stored = record.pop("sha256")
        recomputed = fingerprint(pack(record))
        return {"stored": stored, "recomputed": recomputed,
                "match": stored == recomputed == expected}

    return {
        "confidential": True,
        "package": EXTERNAL_NAME,
        "generated_at": OBSERVE_2,
        "original_exports": {
            "export_1": {
                "path": str(raw1_path.relative_to(REPO_ROOT)),
                "file_sha256": sha256_file(raw1_path),
                "embedded_self_digest": self_check(raw1_path, orig_hash1),
                "observed_at": OBSERVE_1,
            },
            "export_2": {
                "path": str(raw2_path.relative_to(REPO_ROOT)),
                "file_sha256": sha256_file(raw2_path),
                "embedded_self_digest": self_check(raw2_path, orig_hash2),
                "observed_at": OBSERVE_2,
            },
        },
        "sanitized_copies": {
            "export_1": {
                "path": str(ev1.relative_to(REPO_ROOT)),
                "file_sha256": sha256_file(ev1),
                "embedded_self_digest": sanitized1["sha256"],
            },
            "export_2": {
                "path": str(ev2.relative_to(REPO_ROOT)),
                "file_sha256": sha256_file(ev2),
                "embedded_self_digest": sanitized2["sha256"],
            },
        },
        "chain_of_custody": [
            "原始证据由 quaybell.evidence.export_trace 以受控时钟导出，自摘要核对通过",
            "脱敏按 internal/redaction-mapping.CONFIDENTIAL.json 的固定替换表执行",
            "脱敏件自摘要重算通过；脱敏件文件哈希写入对外签名清单",
            "清单使用 internal/keys 下 Ed25519 私钥签名，对应公钥随包分发",
        ],
        "identity_assertion": {
            "message_id": message_id,
            "original_delivery": d1,
            "redriven_successor": d2,
            "parent_link": f"{d2}.parent_id == {d1}",
        },
        "signed_manifest_sha256": sha256_file(manifest),
        "public_key_sha256": sha256_file(pub_key),
    }


if __name__ == "__main__":
    main()
