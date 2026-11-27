#!/usr/bin/env python3
"""R050 本地、可复跑的“同一载荷两次处理”对照实验。

安全边界：
- 只在 TemporaryDirectory 中启动回环 HTTP + SQLite；
- 使用受控 Unix 时钟，不连接现场、不向合作方重发任何消息；
- 所有发布、拒绝、死信、重投和清理都是合成历史。

运行：
    python3 incidents/r050/reproduce.py
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from quaybell.codec import fingerprint, pack  # noqa: E402
from quaybell.persistence import RelayPersistence  # noqa: E402
from quaybell.server import RelayRequest, RelayServer  # noqa: E402

RelayRequest.log_message = lambda self, fmt, *args: None

INCIDENT = "r050"
CHANNEL = "incident-r050"
GROUP = "partner-consumer"
DEVICE = "DEV-24601"
OTHER_DEVICE = "DEV-998877"
BASE = f"/channels/{CHANNEL}"
GROUP_PATH = f"{BASE}/groups/{GROUP}"

# 2026-11-27 00:00:00 +08:00
MIDNIGHT = 1_795_708_800
T_PUBLISH = MIDNIGHT + 75       # 00:01:15
T_FIRST_LOG = MIDNIGHT + 131     # 00:02:11
T_FIRST_NACK = T_FIRST_LOG + 3
T_REDRIVE = MIDNIGHT + 145       # 00:02:25
T_SECOND_LOG = MIDNIGHT + 277    # 00:04:37
T_EXPORT = MIDNIGHT + 1_800      # 00:30:00
T_CLEANUP = MIDNIGHT + 7_200      # 02:00:00

PAYLOAD = {
    "device_id": DEVICE,
    "metric": "cold-chain-temperature",
    "reading": 4.8,
    "unit": "C",
    "observed_at": "2026-11-27T00:02:05+08:00",
}
PAYLOAD_DIGEST = fingerprint(pack(PAYLOAD))
CST = timezone(timedelta(hours=8))


def iso(when: int) -> str:
    return datetime.fromtimestamp(when, CST).isoformat(timespec="seconds")


def known_partner_logs() -> list[dict]:
    """合作方提供的两条日志的共同可观察投影。

    第一条保留分发号/尝试号；第二条的日志格式没有中继身份字段，只剩设备号和摘要。
    """
    return [
        {
            "seq": 1,
            "at": iso(T_FIRST_LOG),
            "delivery_id": 1,
            "attempt": 1,
            "device_id": DEVICE,
            "payload_sha256": PAYLOAD_DIGEST,
        },
        {
            "seq": 2,
            "at": iso(T_SECOND_LOG),
            "device_id": DEVICE,
            "payload_sha256": PAYLOAD_DIGEST,
            # 现场记录中以下字段缺失：
            "delivery_id": None,
            "attempt": None,
            "message_id": None,
        },
    ]


class Clock:
    def __init__(self, value: int):
        self.value = value

    def now(self) -> int:
        return self.value

    def set(self, value: int) -> None:
        self.value = value

    def advance(self, seconds: int) -> None:
        self.value += seconds


class Client:
    def __init__(self, port: int):
        self.base = f"http://127.0.0.1:{port}/v1"

    def call(self, method: str, path: str, body=None):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def expect(self, method: str, path: str, body, expected: int):
        status, payload = self.call(method, path, body)
        if status != expected:
            raise AssertionError(f"{method} {path}: expected {expected}, got {status}: {payload}")
        return payload


class World:
    def __init__(self, name: str, tmp: str, clock: Clock, db_path: Path,
                 server: RelayServer, thread: Thread, client: Client):
        self.name = name
        self.tmp = tmp
        self.clock = clock
        self.db_path = db_path
        self.server = server
        self.thread = thread
        self.client = client


@contextmanager
def world(name: str, policy: dict):
    tmp = tempfile.TemporaryDirectory(prefix=f"quaybell-r050-{name}-")
    clock = Clock(MIDNIGHT)
    db_path = Path(tmp.name) / "relay.sqlite3"
    persistence = RelayPersistence(db_path)
    server = RelayServer(("127.0.0.1", 0), persistence, clock.now)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02},
                    daemon=True)
    thread.start()
    client = Client(server.server_port)
    w = World(name, tmp.name, clock, db_path, server, thread, client)
    try:
        client.expect("POST", "/channels", {"name": CHANNEL, "policy": policy}, 201)
        client.expect("POST", f"{BASE}/groups", {"name": GROUP, "tags": {}}, 201)
        yield w
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
        tmp.cleanup()


def envelope(key: str, device: str = DEVICE) -> dict:
    payload = PAYLOAD if device == DEVICE else {
        "device_id": device,
        "metric": "unrelated-duty-receipt",
        "reading": 99.0,
        "unit": "C",
    }
    return {
        "producer": "device-gateway",
        "key": key,
        "lane": device,
        "tags": {"device-id": device},
        "payload": payload,
    }


def publish(w: World, key: str, *, expected: int = 201, channel: str = CHANNEL,
            device: str = DEVICE):
    return w.client.expect(
        "POST", f"/channels/{channel}/messages", envelope(key, device), expected
    )


def claim_one(w: World, *, channel: str = CHANNEL, group: str = GROUP,
              worker: str = "partner-consumer-a"):
    result = w.client.expect(
        "POST", f"/channels/{channel}/groups/{group}/claim",
        {"worker": worker, "limit": 1}, 200
    )
    if len(result["items"]) != 1:
        raise AssertionError(f"expected one claim, got {result}")
    return result["items"][0]


def claim_ref(item: dict) -> dict:
    return {
        "delivery_id": item["delivery_id"],
        "message_id": item["message_id"],
        "attempt": item["lease"]["number"],
        "lease_expires": item["lease"]["expires"],
        "policy_version": item["policy"],
        "worker_hint": "recorded-by-relay",
    }


def nack(w: World, item: dict, reason: str = "downstream rejected frame"):
    return w.client.expect(
        "POST", f"/deliveries/{item['delivery_id']}/nack",
        {"token": item["lease"]["token"], "reason": reason}, 200
    )


def ack(w: World, item: dict):
    return w.client.expect(
        "POST", f"/deliveries/{item['delivery_id']}/ack",
        {"token": item["lease"]["token"]}, 200
    )


def redrive(w: World, delivery_id: int, expected: int = 201):
    return w.client.expect("POST", f"/deliveries/{delivery_id}/redrive", {}, expected)


def verified_evidence(w: World, message_id: int) -> dict:
    export = w.client.expect("GET", f"/messages/{message_id}/evidence", None, 200)
    unsigned = {key: value for key, value in export.items() if key != "sha256"}
    if fingerprint(pack(unsigned)) != export["sha256"]:
        raise AssertionError("evidence digest mismatch")
    return export


def lineage_summary(export: dict) -> list[dict]:
    return [
        {
            "delivery_id": row["id"],
            "parent_id": row["parent_id"],
            "state": row["state"],
            "policy": row["policy"],
            "attempts": [attempt["number"] for attempt in row["attempts"]],
            "attempt_outcomes": [attempt["outcome"] for attempt in row["attempts"]],
        }
        for row in export["trace"]["deliveries"]
    ]


def unrelated_receipt(w: World) -> dict:
    """生成另一频道中的真实重投回执，模拟值班员无法归属的那张纸。"""
    other_channel = "other-desk"
    other_group = "duty-misc"
    policy = dict(lease_seconds=60, retry_seconds=0, max_attempts=1, retention_seconds=3600)
    w.client.expect("POST", "/channels", {"name": other_channel, "policy": policy}, 201)
    w.client.expect(
        "POST", f"/channels/{other_channel}/groups",
        {"name": other_group, "tags": {}}, 201
    )
    published = publish(
        w, "unrelated-redrive-source", channel=other_channel, device=OTHER_DEVICE
    )
    item = claim_one(w, channel=other_channel, group=other_group, worker="duty-worker")
    failed = nack(w, item, "unrelated synthetic dead letter")
    if failed["state"] != "dead":
        raise AssertionError(f"expected unrelated dead letter: {failed}")
    receipt = redrive(w, published["fanout"][other_group])
    child = claim_one(w, channel=other_channel, group=other_group, worker="duty-worker")
    ack(w, child)
    return {
        "body": receipt,
        "provenance_channel": other_channel,
        "provenance_database": "same local synthetic relay file",
        "note": "回执响应本身没有 channel、device_id、message_id 或 payload 摘要",
    }


def db_query(db_path: Path, sql: str, *args):
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(sql, args).fetchall()]
    finally:
        connection.close()


def db_lookup_delivery(db_path: Path, delivery_id: int | None):
    if delivery_id is None:
        return None
    rows = db_query(
        db_path,
        """
        SELECT d.id AS delivery_id, d.message_id, d.parent_id, m.channel, d.state
        FROM deliveries d JOIN messages m ON m.id=d.message_id
        WHERE d.id=?
        """,
        delivery_id,
    )
    return rows[0] if rows else None


def post_cleanup_snapshot(w: World, removed: list[int], receipt: dict | None) -> dict:
    counts = {}
    for name, sql in {
        "messages": "SELECT COUNT(*) AS n FROM messages WHERE channel=?",
        "deliveries": """
            SELECT COUNT(*) AS n FROM deliveries d JOIN messages m ON m.id=d.message_id
            WHERE m.channel=?
        """,
        "attempts": """
            SELECT COUNT(*) AS n
            FROM attempts a JOIN deliveries d ON d.id=a.delivery_id
            JOIN messages m ON m.id=d.message_id WHERE m.channel=?
        """,
        "message_level_journal": """
            SELECT COUNT(*) AS n FROM journal
            WHERE channel=? AND message_id IS NOT NULL
        """,
    }.items():
        counts[name] = db_query(w.db_path, sql, CHANNEL)[0]["n"]

    pruned_rows = db_query(
        w.db_path,
        "SELECT id,at,kind,details FROM journal WHERE channel=? AND kind='pruned' ORDER BY id",
        CHANNEL,
    )
    pruned = [
        {"id": row["id"], "at": row["at"], "kind": row["kind"],
         "details": json.loads(row["details"])}
        for row in pruned_rows
    ]
    kinds = db_query(
        w.db_path,
        "SELECT kind, COUNT(*) AS count FROM journal WHERE channel=? GROUP BY kind ORDER BY kind",
        CHANNEL,
    )
    receipt_child = receipt["body"].get("delivery_id") if receipt else None
    return {
        "prune_response": {"removed": removed, "observed_at": w.clock.now()},
        "target_channel_counts_after_delete": counts,
        "surviving_journal_kinds": kinds,
        "surviving_prune_record": pruned,
        "receipt_child_lookup_in_current_db": db_lookup_delivery(w.db_path, receipt_child),
        "interpretation": (
            "消息、分发、尝试和消息级事件被级联删除；无 message_id 的频道级 pruned 记录保留。"
        ),
    }


def evidence_ladder(identity: dict) -> dict:
    """根据补到的中继身份，逐步给出可排除项。"""
    delivery_id = identity["delivery_id"]
    attempt = identity["attempt"]
    message_id = identity["message_id"]
    parent_id = identity["parent_id"]

    if delivery_id == 1 and attempt == 1:
        raw_log = {
            "observation": {"delivery_id": delivery_id, "attempt": attempt},
            "rules_out": ["new_publication", "old_delivery_retry",
                          "redrive_new_delivery"],
            "supports": "same_attempt_duplicate_processing_or_log",
            "decisive": True,
            "reason": "中继的下一次领取会生成 attempt=2；相同 D/A 的两条处理记录应查消费者内部重试、日志重复或应答回执重试。",
        }
    elif delivery_id == 1 and attempt and attempt > 1:
        raw_log = {
            "observation": {"delivery_id": delivery_id, "attempt": attempt},
            "rules_out": ["new_publication", "redrive_new_delivery"],
            "supports": "old_delivery_retry",
            "decisive": True,
        }
    elif delivery_id != 1:
        raw_log = {
            "observation": {"delivery_id": delivery_id, "attempt": attempt},
            "rules_out": ["old_delivery_retry"],
            "still_open": ["new_publication", "redrive_new_delivery"],
            "decisive": False,
            "reason": "新分发号和 attempt=1 同时符合新发布与重投后继。",
        }
    else:
        raw_log = {"decisive": False}

    if message_id == 1:
        message_step = {
            "observation": {"message_id": message_id},
            "rules_out": ["new_publication"],
            "still_open": ["old_delivery_retry", "redrive_new_delivery"],
            "decisive": False,
        }
    else:
        message_step = {
            "observation": {"message_id": message_id},
            "rules_out": ["redrive_of_known_delivery_1"],
            "still_open": ["new_publication", "redrive_of_unknown_other_source"],
            "decisive": False,
            "reason": "重投不换 message_id；但若记录不完整，仍需排除另一条同载荷消息。",
        }

    if parent_id == 1 and message_id == 1 and delivery_id != 1:
        lineage_step = {
            "observation": {"message_id": message_id, "delivery_id": delivery_id,
                            "parent_id": parent_id},
            "rules_out": ["new_publication", "old_delivery_retry"],
            "confirms": "redrive_new_delivery",
            "decisive": True,
        }
    elif parent_id is None and message_id != 1:
        lineage_step = {
            "observation": {"message_id": message_id, "delivery_id": delivery_id,
                            "parent_id": None},
            "rules_out": ["old_delivery_retry", "redrive_of_known_delivery_1"],
            "confirms": "new_publication_when_publish_ledger_matches",
            "decisive": True,
        }
    elif parent_id is None and delivery_id == 1 and attempt and attempt > 1:
        lineage_step = {
            "observation": {"delivery_id": delivery_id, "parent_id": None,
                            "attempt": attempt},
            "confirms": "same_delivery_retry",
            "rules_out": ["new_publication", "redrive_new_delivery"],
            "decisive": True,
        }
    elif parent_id is None and delivery_id == 1 and attempt == 1:
        lineage_step = {
            "observation": {"delivery_id": delivery_id, "parent_id": None,
                            "attempt": attempt},
            "rules_out": ["new_publication", "old_delivery_retry",
                          "redrive_new_delivery"],
            "indicates": "same_attempt_duplicate_or_log_duplication",
            "decisive": True,
        }
    else:
        lineage_step = {"decisive": False}

    return {
        "device_and_digest_only": {
            "rules_out": [],
            "raises_suspicion_for": ["new_publication", "old_delivery_retry",
                                     "redrive_new_delivery"],
            "reason": "三种历史都能发布/携带完全相同的规范化载荷。",
        },
        "unlinked_redrive_receipt": {
            "rules_out": [],
            "raises_suspicion_for": ["redrive_new_delivery"],
            "reason": "回执没有设备、载荷、消息号或时间；不与第二条日志的中继 ID 连接就没有排除力。",
        },
        "complete_second_consumer_log": raw_log,
        "message_identity": message_step,
        "lineage_or_pre_cleanup_trace": lineage_step,
    }


def assert_classification(name: str, identity: dict, expected: str) -> None:
    ladder = evidence_ladder(identity)
    lineage = ladder["lineage_or_pre_cleanup_trace"]
    if expected == "old_delivery_retry":
        assert ladder["complete_second_consumer_log"]["supports"] == expected
        assert lineage["confirms"] == "same_delivery_retry"
    elif expected == "redrive_new_delivery":
        assert lineage["confirms"] == expected
        assert set(lineage["rules_out"]) == {"new_publication", "old_delivery_retry"}
    elif expected == "new_publication":
        assert lineage["confirms"] == "new_publication_when_publish_ledger_matches"
        assert "old_delivery_retry" in lineage["rules_out"]
    else:
        raise AssertionError(expected)


def case_retry() -> dict:
    policy = dict(lease_seconds=300, retry_seconds=120, max_attempts=3,
                  retention_seconds=60)
    with world("retry", policy) as w:
        w.clock.set(T_PUBLISH)
        first_publish = publish(w, "gw-24601-20261127-0001")
        assert first_publish == {
            "message_id": 1, "policy": 1, "repeated": False,
            "fanout": {GROUP: 1},
        }

        w.clock.set(T_FIRST_LOG)
        first_claim = claim_one(w, worker="partner-consumer-a")
        first_ref = claim_ref(first_claim)
        assert first_ref["delivery_id"] == 1 and first_ref["attempt"] == 1

        w.clock.set(T_FIRST_NACK)
        rejected = nack(w, first_claim)
        assert rejected["state"] == "waiting"
        assert rejected["effective_at"] == T_FIRST_NACK + 120

        w.clock.set(T_SECOND_LOG)
        second_claim = claim_one(w, worker="partner-consumer-b")
        second_ref = claim_ref(second_claim)
        assert second_ref["delivery_id"] == 1
        assert second_ref["message_id"] == 1
        assert second_ref["attempt"] == 2

        acked = ack(w, second_claim)
        assert acked["state"] == "acked"

        w.clock.set(T_EXPORT)
        export = verified_evidence(w, 1)
        receipt = unrelated_receipt(w)
        identity = {"message_id": 1, "delivery_id": 1, "attempt": 2, "parent_id": None}
        assert_classification("retry", identity, "old_delivery_retry")

        w.clock.set(T_CLEANUP)
        removed = w.client.expect("POST", f"{BASE}/retention", {}, 200)["removed"]
        assert removed == [1]
        status, _ = w.client.call("GET", "/messages/1")
        assert status == 404
        snapshot = post_cleanup_snapshot(w, removed, receipt)

        return {
            "title": "历史 A：旧分发 D1 的第 2 次尝试造成第二次处理",
            "known_evidence_projection": known_partner_logs(),
            "hidden_second_claim_if_raw_log_recovered": second_ref,
            "first_claim": first_ref,
            "publish_receipts": [first_publish],
            "duty_receipt": receipt,
            "identity": identity,
            "lineage_before_cleanup": lineage_summary(export),
            "evidence_sha256_before_cleanup": export["sha256"],
            "evidence_ladder": evidence_ladder(identity),
            "post_cleanup": snapshot,
        }


def case_redrive() -> dict:
    policy = dict(lease_seconds=300, retry_seconds=3, max_attempts=1,
                  retention_seconds=60)
    with world("redrive", policy) as w:
        w.clock.set(T_PUBLISH)
        first_publish = publish(w, "gw-24601-20261127-0001")
        assert first_publish["fanout"][GROUP] == 1

        w.clock.set(T_FIRST_LOG)
        first_claim = claim_one(w)
        first_ref = claim_ref(first_claim)
        assert first_ref["attempt"] == 1

        w.clock.set(T_FIRST_NACK)
        dead = nack(w, first_claim)
        assert dead["state"] == "dead"

        w.clock.set(T_REDRIVE)
        receipt_body = redrive(w, 1)
        assert receipt_body == {
            "source_id": 1, "delivery_id": 2, "policy": 1, "repeated": False
        }
        # 重复点击/重试重投不会产生第二个后继。
        assert redrive(w, 1, expected=200) == receipt_body | {"repeated": True}

        w.clock.set(T_SECOND_LOG)
        second_claim = claim_one(w, worker="partner-consumer-c")
        second_ref = claim_ref(second_claim)
        assert second_ref["delivery_id"] == 2
        assert second_ref["message_id"] == 1
        assert second_ref["attempt"] == 1
        assert second_ref["policy_version"] == 1

        acked = ack(w, second_claim)
        assert acked["state"] == "acked"

        w.clock.set(T_EXPORT)
        export = verified_evidence(w, 1)
        identity = {"message_id": 1, "delivery_id": 2, "attempt": 1, "parent_id": 1}
        assert_classification("redrive", identity, "redrive_new_delivery")
        assert any(
            event["kind"] == "redriven" and event["details"] == receipt_body
            for event in export["trace"]["events"]
        )

        receipt = {
            "body": receipt_body,
            "provenance_channel": CHANNEL,
            "provenance_database": "the synthetic incident DB",
            "note": "这张回执在本历史中确实对应第二条日志，但第二条日志本身没有 D2 可与之连接。",
        }

        w.clock.set(T_CLEANUP)
        removed = w.client.expect("POST", f"{BASE}/retention", {}, 200)["removed"]
        assert removed == [1]
        status, _ = w.client.call("GET", "/messages/1")
        assert status == 404
        snapshot = post_cleanup_snapshot(w, removed, receipt)

        return {
            "title": "历史 B：D1 耗尽后死信，重投追加 D2，D2 attempt=1 造成第二次处理",
            "known_evidence_projection": known_partner_logs(),
            "hidden_second_claim_if_raw_log_recovered": second_ref,
            "first_claim": first_ref,
            "publish_receipts": [first_publish],
            "duty_receipt": receipt,
            "identity": identity,
            "lineage_before_cleanup": lineage_summary(export),
            "evidence_sha256_before_cleanup": export["sha256"],
            "evidence_ladder": evidence_ladder(identity),
            "post_cleanup": snapshot,
        }


def case_new_publication() -> dict:
    policy = dict(lease_seconds=300, retry_seconds=120, max_attempts=3,
                  retention_seconds=60)
    with world("new-publication", policy) as w:
        w.clock.set(T_PUBLISH)
        first_publish = publish(w, "gw-24601-20261127-0001")

        # 同 producer + key + 完全相同信封的应用侧重发，只返回同一 M1/D1。
        repeated_publish = publish(w, "gw-24601-20261127-0001", expected=200)
        assert repeated_publish == first_publish | {"repeated": True}

        w.clock.set(T_FIRST_LOG)
        first_claim = claim_one(w)
        first_ref = claim_ref(first_claim)
        w.clock.set(T_FIRST_NACK)
        ack(w, first_claim)

        w.clock.set(MIDNIGHT + 230)
        second_publish = publish(w, "gw-24601-20261127-0002")
        assert second_publish["message_id"] == 2
        assert second_publish["fanout"][GROUP] == 2

        w.clock.set(T_SECOND_LOG)
        second_claim = claim_one(w, worker="partner-consumer-d")
        second_ref = claim_ref(second_claim)
        assert second_ref["message_id"] == 2
        assert second_ref["delivery_id"] == 2
        assert second_ref["attempt"] == 1
        ack(w, second_claim)

        w.clock.set(T_EXPORT)
        export_one = verified_evidence(w, 1)
        export_two = verified_evidence(w, 2)
        receipt = unrelated_receipt(w)
        identity = {"message_id": 2, "delivery_id": 2, "attempt": 1, "parent_id": None}
        assert_classification("new", identity, "new_publication")

        w.clock.set(T_CLEANUP)
        removed = w.client.expect("POST", f"{BASE}/retention", {}, 200)["removed"]
        assert removed == [1, 2]
        for message_id in (1, 2):
            status, _ = w.client.call("GET", f"/messages/{message_id}")
            assert status == 404
        snapshot = post_cleanup_snapshot(w, removed, receipt)

        return {
            "title": "历史 C：另一个生产键发起真正的新发布，生成 M2/D2 attempt=1",
            "known_evidence_projection": known_partner_logs(),
            "hidden_second_claim_if_raw_log_recovered": second_ref,
            "first_claim": first_ref,
            "publish_receipts": [first_publish, repeated_publish, second_publish],
            "same_key_republish_control": {
                "request": "same producer/key and identical envelope before cleanup",
                "response": repeated_publish,
                "new_delivery_created": False,
            },
            "duty_receipt": receipt,
            "identity": identity,
            "lineage_before_cleanup": {
                "message_1": lineage_summary(export_one),
                "message_2": lineage_summary(export_two),
            },
            "evidence_sha256_before_cleanup": {
                "message_1": export_one["sha256"],
                "message_2": export_two["sha256"],
            },
            "evidence_ladder": evidence_ladder(identity),
            "post_cleanup": snapshot,
        }


def case_key_reuse_after_cleanup() -> dict:
    """验证清理后同一生产键可被新消息复用；只有键名不能排除新发布。"""
    policy = dict(lease_seconds=60, retry_seconds=0, max_attempts=1,
                  retention_seconds=60)
    t0 = 1_800_100_000
    with world("key-reuse", policy) as w:
        w.clock.set(t0)
        first = publish(w, "reusable-key")
        item = claim_one(w)
        w.clock.set(t0 + 5)
        ack(w, item)

        w.clock.set(t0 + 61)
        first_prune = w.client.expect("POST", f"{BASE}/retention", {}, 200)
        assert first_prune["removed"] == [1]
        status, _ = w.client.call("GET", "/messages/1")
        assert status == 404

        w.clock.set(t0 + 120)
        second = publish(w, "reusable-key", expected=201)
        assert second["repeated"] is False
        assert second["message_id"] == 2
        assert second["fanout"][GROUP] == 2
        second_item = claim_one(w)
        second_ref = claim_ref(second_item)
        ack(w, second_item)

        return {
            "title": "对照 D：清理后复用同一 producer/key 会形成新消息，而不是 repeated 回执",
            "first_publish": first,
            "prune_at": t0 + 61,
            "second_publish_at": t0 + 120,
            "second_log_at": t0 + 120,
            "second_publish_after_prune": second,
            "second_claim": second_ref,
            "temporal_exclusion": {
                "rule": "second_log_at > prune_at excludes retry/redrive of pruned M1",
                "passed": t0 + 120 > t0 + 61 and second["message_id"] != first["message_id"],
            },
            "timeline_rule": (
                "若可信的第二条日志时间晚于删除 M1 的 pruned 记录时间，"
                "M1 的旧重试/重投在该库中已不可能；应转向新发布或其他来源。"
            ),
        }


def verification_report(worlds: dict, reuse_control: dict) -> list[dict]:
    checks: list[dict] = []

    def check(name: str, passed: bool, **detail) -> None:
        if not passed:
            raise AssertionError(name)
        checks.append({"check": name, "passed": True, **detail})

    expected_logs = known_partner_logs()
    for name, world in worlds.items():
        check(f"{name}: same残缺日志投影",
              world["known_evidence_projection"] == expected_logs)

        counts = world["post_cleanup"]["target_channel_counts_after_delete"]
        check(f"{name}: relay rows are pruned",
              counts["messages"] == counts["deliveries"] == counts["attempts"] == 0)
        check(f"{name}: channel-level pruned journal survives",
              len(world["post_cleanup"]["surviving_prune_record"]) == 1)

        receipt_keys = set(world["duty_receipt"]["body"])
        check(f"{name}: receipt alone lacks join keys",
              receipt_keys == {"source_id", "delivery_id", "policy", "repeated"},
              receipt_keys=sorted(receipt_keys))

    retry_ladder = worlds["A_old_delivery_retry"]["evidence_ladder"]
    check("same D + larger attempt rules out publish/redrive",
          retry_ladder["complete_second_consumer_log"]["rules_out"]
          == ["new_publication", "redrive_new_delivery"],
          confirms="old_delivery_retry")

    same_attempt = evidence_ladder(
        {"message_id": 1, "delivery_id": 1, "attempt": 1, "parent_id": None}
    )
    check("same D + same A rules out all three relay replacement histories",
          len(same_attempt["complete_second_consumer_log"]["rules_out"]) == 3,
          indicates="same_attempt_duplicate_processing_or_log")

    redrive_ladder = worlds["B_redrive_new_delivery"]["evidence_ladder"]
    check("same M + parent D1 rules out publish/retry",
          redrive_ladder["lineage_or_pre_cleanup_trace"]["decisive"] is True,
          confirms="redrive_new_delivery")

    new_ladder = worlds["C_new_publication"]["evidence_ladder"]
    check("new M + parent NULL plus ledger rules out known lineage",
          new_ladder["lineage_or_pre_cleanup_trace"]["decisive"] is True,
          confirms="new_publication")

    check("post-prune timestamp makes old M1 lineage impossible",
          reuse_control["temporal_exclusion"]["passed"] is True,
          prune_at=reuse_control["prune_at"],
          second_log_at=reuse_control["second_log_at"])
    return checks


def evidence_matrix() -> list[dict]:
    return [
        {
            "evidence": "设备号相同 + 载荷 SHA-256/规范化摘要相同 + 时间接近",
            "can_rule_out": [],
            "only_raises_suspicion_for": [
                "new_publication", "old_delivery_retry", "redrive_new_delivery"
            ],
            "why": "三个合成历史使用同一 PAYLOAD 均可复现。",
        },
        {
            "evidence": "值班员持有的重投回执，但未与第二条日志的 delivery_id/时间连接",
            "can_rule_out": [],
            "only_raises_suspicion_for": ["redrive_new_delivery"],
            "why": "Quaybell 回执体只有 source_id、delivery_id、policy、repeated；没有设备、载荷、message_id、频道或时间。",
        },
        {
            "evidence": "合作方原始日志补全：第二条仍是 D1，attempt=2（或更大）",
            "can_rule_out": ["new_publication", "redrive_new_delivery"],
            "supports": "old_delivery_retry",
            "why": "新发布和重投都追加新的 delivery 行；旧尝试只增加 attempt。",
        },
        {
            "evidence": "合作方原始日志补全：第二条仍是 D1，且 attempt 与第一条相同（同为 A1）",
            "can_rule_out": ["new_publication", "old_delivery_retry", "redrive_new_delivery"],
            "supports": "same_attempt_duplicate_processing_or_log",
            "why": "中继重试会生成更大的 attempt；相同 D/A 的重复更像消费者内部重试、日志重复或 ack 回执重试。",
        },
        {
            "evidence": "合作方原始日志补全：第二条是 D2、attempt=1，但没有 message_id/parent",
            "can_rule_out": ["old_delivery_retry"],
            "still_cannot_distinguish": ["new_publication", "redrive_new_delivery"],
            "why": "两种机制都会产生新的 delivery_id 且首次尝试为 1。",
        },
        {
            "evidence": "清理前 evidence 导出/备份显示两条处理共享 M1",
            "can_rule_out": ["new_publication"],
            "still_cannot_distinguish_without_delivery_or_parent": [
                "old_delivery_retry", "redrive_new_delivery"
            ],
            "why": "真正的新发布有新 message_id；重投保留原 message_id。",
        },
        {
            "evidence": "M1 trace/evidence 显示 D2.parent_id=D1 且有 redriven 事件，D2 对应第二条日志",
            "can_rule_out": ["new_publication", "old_delivery_retry"],
            "confirms": "redrive_new_delivery",
            "why": "谱系和 redriven 审计事件把回执、后继分发与第二条日志连起来。",
        },
        {
            "evidence": "生产者 outbox/发布回执/访问日志显示第二个生产身份或清理后的新 M2，且其 fanout D2 对应第二条日志",
            "can_rule_out": ["old_delivery_retry", "redrive_new_delivery"],
            "confirms": "new_publication",
            "why": "重试/重投不会创建新的生产消息；同键同信封在清理前只会 repeated 到原 M1。",
        },
        {
            "evidence": "当前库幸存的 pruned 频道日志 + 可信时钟证明第二条日志晚于该 M1 的删除事务",
            "can_rule_out": ["old_delivery_retry_of_M1", "redrive_of_M1"],
            "supports": "new_publication_or_other_source",
            "why": "messages 删除级联删除 deliveries/attempts；删除后旧 ID 不可能再被领取，原生产键可复用。结论以现网库未被清理前快照回滚为前提。",
        },
        {
            "evidence": "同一 worker/IP、相似堆栈或业务报错文案",
            "can_rule_out": [],
            "only_raises_suspicion_for": [],
            "why": "租约到期、nack 后换 worker、重投后换 worker 都可能出现相同或不同执行位置；不是身份证据。",
        },
        {
            "evidence": "evidence JSON 重算 sha256 一致",
            "can_rule_out": [],
            "why": "只能证明拿到的导出未损坏/与摘要自洽；SHA 不是外部签名，仍需从可信导出桶、值班终端或备份证明来源。",
        },
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_out = Path(__file__).with_name("transcript.json")
    parser.add_argument("--out", type=Path, default=default_out,
                        help="写入机器可读对照结果的 JSON 路径")
    args = parser.parse_args()

    worlds = {
        "A_old_delivery_retry": case_retry(),
        "B_redrive_new_delivery": case_redrive(),
        "C_new_publication": case_new_publication(),
    }
    key_reuse = case_key_reuse_after_cleanup()
    controls = {
        "same_identity_projection_asserted_for": list(worlds),
        "same_key_before_cleanup": worlds["C_new_publication"]["same_key_republish_control"],
        "production_key_reuse_after_cleanup": key_reuse,
    }
    verification = verification_report(worlds, key_reuse)
    result = {
        "scenario": INCIDENT,
        "mode": "local-synthetic-no-production-resend",
        "clock": "fixed controlled Unix clock",
        "incident_midnight_cst": iso(MIDNIGHT),
        "payload_digest_algorithm": "sha256(quaybell.codec.pack(payload)); sort_keys and compact JSON",
        "payload_digest": PAYLOAD_DIGEST,
        "known_evidence": {
            "partner_logs": known_partner_logs(),
            "duty_receipt": "one unjoined redrive receipt; body fields are source_id/delivery_id/policy/repeated",
            "relay_details": "original message details have been pruned in the incident premise",
        },
        "worlds": worlds,
        "controls": controls,
        "verification_checks": verification,
        "evidence_matrix": evidence_matrix(),
        "first_contact_order": [
            {
                "who": "合作方值班/日志管理员",
                "ask_for": (
                    "不要重发；导出午夜窗口两条原始日志及完整字段：message_id、delivery_id、"
                    "lease/attempt.number、worker、trace-id、精确时间戳、日志采集时间和摘要算法。"
                ),
                "why_first": "第二条日志的中继 ID 是区分三种机制成本最低的钥匙。",
            },
            {
                "who": "本方持有重投回执的值班员",
                "ask_for": (
                    "回执原件/截图/响应体、来源环境与频道、执行时间、操作人、终端或工单；"
                    "核对 source_id/delivery_id/repeated，并寻找当时的 evidence 导出。"
                ),
                "why_second": "可立即判断回执是否与第一条 D1 同源，以及应查哪个 M1/备份。",
            },
            {
                "who": "Quaybell 库 owner/DBA 或备份负责人",
                "ask_for": (
                    "当前 SQLite、WAL、清理任务日志、清理前快照/备份、对象存储中的 quaybell-trace/1；"
                    "先查无 message_id 的 journal pruned 记录，不要继续清理或覆盖文件。"
                ),
                "why_third": "当前库可能只剩 pruned 消息 ID；完整谱系通常要靠导出或清理前备份。",
            },
            {
                "who": "生产者/网关负责人",
                "ask_for": (
                    "同一设备与摘要在窗口内的 outbox、发布回执、幂等键、producer/key、"
                    "HTTP 访问日志和响应 message_id/fanout。"
                ),
                "why_fourth": "用于证明是否存在第二个生产身份，以及清理后是否复用了旧键。",
            },
        ],
        "all_checks_passed": True,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"R050 对照实验通过；结果已写入 {args.out}")
    print(f"payload_sha256={PAYLOAD_DIGEST}")
    for key, value in worlds.items():
        identity = value["identity"]
        print(f"{key}: M{identity['message_id']}/D{identity['delivery_id']}/A{identity['attempt']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
