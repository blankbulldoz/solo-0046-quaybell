#!/usr/bin/env python3
"""Quaybell 冷备放行验收工具：对恢复实例只走公共 HTTP 接口取证与判定。

仅依赖 Python 标准库。原则：先固定全部取件前证据，再执行任何取件；绝不在
租约仍然有效时拿在手凭据做 ack，以免改写现场。

放行方只持有事故前留存的两份事实：
  1. 重投回执 /redrive 的响应（含 source_id 与后继 delivery_id）
  2. 另一条在手租约的取件事实（claim 响应，含 lease.token 与 expires）

verdict:
  reject_wrong_backup            选错备份：死信还在，但重投回执指向的后继不存在
  hold_reaped_after_restore      恢复对了，但取件回收已把在手投递从 leased 推进
  release_clean_restore          恢复点正确，后继仍未处置且无回收推进，可放行

退出码: 0=可放行 2=选错备份 3=回收推进挂起
"""
import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path

WRONG = "reject_wrong_backup"
REAPED = "hold_reaped_after_restore"
CLEAN = "release_clean_restore"


def call(base: str, method: str, path: str, payload=None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.code, json.load(response)
    except urllib.error.HTTPError as exc:
        with exc:
            return exc.code, json.load(exc)


def save(directory: Path, name: str, payload) -> Path:
    path = directory / name
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")
    return path


def find_delivery(trace: dict, delivery_id: int):
    for delivery in trace.get("deliveries", []):
        if delivery["id"] == delivery_id:
            return delivery
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Quaybell 冷备恢复放行验收")
    parser.add_argument("--base", required=True, help="恢复实例的 /v1 基地址")
    parser.add_argument("--channel", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--redrive-receipt", required=True, type=Path)
    parser.add_argument("--lease-fact", required=True, type=Path)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    args = parser.parse_args()

    evidence = args.evidence_dir
    evidence.mkdir(parents=True, exist_ok=True)
    receipt = json.loads(args.redrive_receipt.read_text(encoding="utf-8"))
    lease_fact = json.loads(args.lease_fact.read_text(encoding="utf-8"))
    save(evidence, "00-input-redrive-receipt.json", receipt)
    save(evidence, "00-input-lease-fact.json", lease_fact)

    source_id = int(receipt["source_id"])
    successor_id = int(receipt["delivery_id"])
    leased_delivery = int(lease_fact["delivery_id"])
    lease_message_id = int(lease_fact["message_id"])
    stale_token = lease_fact["lease"]["token"]
    group_path = f"/channels/{args.channel}/groups/{args.group}"
    checks = {"redrive_receipt": {"source_id": source_id, "successor_id": successor_id},
              "lease_fact": {"delivery_id": leased_delivery, "message_id": lease_message_id,
                             "lease_expires": lease_fact["lease"]["expires"]}}

    # ---- 第一步：取件前固定证据（全部 GET，不做任何写入） ----
    board_code, board_before = call(args.base, "GET", f"/channels/{args.channel}")
    save(evidence, "01-board-before-claim.json", board_before)
    observed_now = board_before["observed_at"]

    dead_code, dead_before = call(args.base, "GET", f"/channels/{args.channel}/dead")
    save(evidence, "02-dead-before-claim.json", dead_before)
    source_entry = next((item for item in dead_before["items"] if item["id"] == source_id), None)

    trace_lease_code, trace_lease_before = call(args.base, "GET", f"/messages/{lease_message_id}")
    save(evidence, "03-lease-message-trace-before-claim.json", trace_lease_before)
    leased_item = find_delivery(trace_lease_before, leased_delivery)
    checks["leased_delivery_state_before_claim"] = leased_item["state"] if leased_item else None

    # 死信条目自带 message_id 与 redriven_as，可直接做谱系核对并定位后继所属消息
    checks["dead_entry_present"] = source_entry is not None
    checks["redriven_as_in_backup"] = source_entry["redriven_as"] if source_entry else None
    if source_entry is None or source_entry.get("redriven_as") != successor_id:
        verdict = {"verdict": WRONG,
                   "reason": "重投回执指向的后继投递在恢复库中不存在（死信 redriven_as 为空），"
                             "该备份取于死信重投之前，选错备份",
                   "checks": checks, "claim_performed": False}
        save(evidence, "99-verdict.json", verdict)
        print(json.dumps(verdict, ensure_ascii=False, indent=2))
        return 2

    source_message_id = source_entry["message_id"]
    trace_src_code, trace_src_before = call(args.base, "GET", f"/messages/{source_message_id}")
    save(evidence, "04-redrive-message-trace-before-claim.json", trace_src_before)
    export_code, export_src = call(args.base, "GET", f"/messages/{source_message_id}/evidence")
    save(evidence, "05-redrive-message-evidence-before-claim.json", export_src)
    export_lease_code, export_lease = call(args.base, "GET", f"/messages/{lease_message_id}/evidence")
    save(evidence, "06-lease-message-evidence-before-claim.json", export_lease)

    # 同键（同顺序键/lane）后继的阻挡证据：直接取自控制台 board 样本
    same_lane = [row for row in board_before["groups"]
                 for row in row.get("sample", [])]
    save(evidence, "07-lane-heads-before-claim.json",
         [{"delivery_id": row["id"], "lane": row["lane"], "state": row["state"],
           "cause": row["cause"], "head_id": row["head_id"]} for row in same_lane])

    successor = find_delivery(trace_src_before, successor_id)
    checks["successor_state_before_claim"] = successor["state"] if successor else None
    checks["successor_attempt_count"] = successor["attempt_count"] if successor else None

    # ---- 第二步：只依据证据判断在手租约是否到期，到期才用原凭据试探（预期 409） ----
    lease_expires = float(lease_fact["lease"]["expires"])
    lease_expired = lease_expires <= float(observed_now)
    checks["server_observed_at"] = observed_now
    checks["lease_expired_according_to_evidence"] = lease_expired
    if lease_expired:
        ack_code, ack_body = call(args.base, "POST", f"/deliveries/{leased_delivery}/ack",
                                  {"token": stale_token})
        save(evidence, "08-stale-token-ack-result.json",
             {"http_status": ack_code, "body": ack_body})
        checks["stale_token_ack_status"] = ack_code
    else:
        checks["stale_token_ack_status"] = "skipped-live-lease-not-mutated"

    # ---- 第三步：执行取件（claim 先回收本组最多 100 个到期尝试，再放行队头） ----
    claim_code, claim_body = call(args.base, "POST", group_path + "/claim",
                                  {"worker": "cold-standby-acceptance", "limit": 10})
    save(evidence, "09-claim-response.json", {"http_status": claim_code, "body": claim_body})
    checks["claimed_delivery_ids"] = [item["delivery_id"]
                                      for item in claim_body.get("items", [])]

    # ---- 第四步：取件后再次固定证据，比对状态推进与同键阻挡 ----
    _, board_after = call(args.base, "GET", f"/channels/{args.channel}")
    save(evidence, "10-board-after-claim.json", board_after)
    _, trace_lease_after = call(args.base, "GET", f"/messages/{lease_message_id}")
    save(evidence, "11-lease-message-trace-after-claim.json", trace_lease_after)
    _, trace_src_after = call(args.base, "GET", f"/messages/{source_message_id}")
    save(evidence, "12-redrive-message-trace-after-claim.json", trace_src_after)

    leased_after = find_delivery(trace_lease_after, leased_delivery)
    successor_after = find_delivery(trace_src_after, successor_id)
    checks["leased_delivery_state_after_claim"] = leased_after["state"] if leased_after else None
    checks["successor_state_after_claim"] = successor_after["state"] if successor_after else None
    events_before = [event["kind"] for event in trace_lease_before.get("events", [])]
    events_after = [event["kind"] for event in trace_lease_after.get("events", [])]
    checks["lease_message_event_kinds"] = events_after
    # 租约被回收一定会在消息级审计留下 expired 事件；即便同一次取件又把它重新领取
    # （最终状态回到 leased），expired 事件数的变化仍是回收推进的可靠信号。
    expired_before = events_before.count("expired")
    expired_after = events_after.count("expired")
    checks["expired_events_before_claim"] = expired_before
    checks["expired_events_after_claim"] = expired_after

    # 回收推进有两种形态：
    #   A. 取件前无 expired 事件（原租约仍 leased），本次 claim 的 reap 产生了 expired 事件
    #   B. 恢复实例到手时该租约已被早先的取件/回收处置，取件前观察就已有 expired 事件
    already_reaped_before_claim = expired_before > 0
    reaped_during_claim = expired_after > expired_before
    reaped = already_reaped_before_claim or reaped_during_claim
    checks["reaped_during_claim"] = reaped_during_claim
    checks["already_reaped_before_claim"] = already_reaped_before_claim
    successor_unhandled = (successor is not None and successor["state"] == "waiting"
                           and successor["attempt_count"] == 0)
    checks["reap_detected"] = reaped
    checks["successor_unhandled_before_claim"] = successor_unhandled

    if reaped:
        if already_reaped_before_claim:
            reason = ("备份谱系正确（重投后继存在且未处置），但在手租约在取件前的首次观察时"
                      "已是回收后状态（存在 expired 事件，当前为 "
                      f"{leased_after['state']}），本次取件会把它重新领取，"
                      "须按重处理流程复核后再放行")
        else:
            reason = ("备份谱系正确（重投后继存在且未处置），但原租约已到期，"
                      "本次取件先触发回收把在手投递由 leased 推进为 "
                      f"{leased_after['state']}，须按重处理流程复核后再放行")
        verdict = {"verdict": REAPED, "reason": reason,
                   "checks": checks, "claim_performed": True}
        exit_code = 3
    elif successor_unhandled:
        verdict = {"verdict": CLEAN,
                   "reason": "备份谱系正确，重投后继仍为未处置等待态，在手租约未到期且无回收推进，可放行",
                   "checks": checks, "claim_performed": True}
        exit_code = 0
    else:
        verdict = {"verdict": REAPED,
                   "reason": "恢复点状态与取件前事实不一致，按回收推进嫌疑挂起复核",
                   "checks": checks, "claim_performed": True}
        exit_code = 3

    save(evidence, "99-verdict.json", verdict)
    print(json.dumps(verdict, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
