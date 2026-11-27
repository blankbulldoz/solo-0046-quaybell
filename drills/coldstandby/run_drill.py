#!/usr/bin/env python3
"""Quaybell 冷备与放行演练编排器（可重跑）。

用受控时钟起真实 RelayServer（公共 HTTP 入口），构造同一频道下两份消息条数与
最大消息号完全相同、但取点不同的冷备：
  backup-early   取于死信重投之前（选错备份的现场）
  backup-correct 取于死信已重投、后继尚未处置之时（正确备份）

每个场景各恢复三次到全新空目录，分别覆盖：
  选错备份 / 恢复对了但回收推进了状态 / 恢复对且租约未到期的干净放行
并在另一条消息存在同键（同 lane）后继时重复整套演练。

运行（从仓库根目录）:
  python3 -I drills/coldstandby/run_drill.py --workdir drills/coldstandby/runtime
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path
from threading import Thread

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from quaybell.server import RelayServer  # noqa: E402
from quaybell.persistence import RelayPersistence  # noqa: E402

CHANNEL = "cold-drill"
GROUP = "rotor"
GROUP_PATH = f"/channels/{CHANNEL}/groups/{GROUP}"
# lease=120s 让在手租约（holder）在整个现场准备阶段保持有效；retry=3s、max_attempts=2。
# 死信走 nack 退避路径（无需等租约超时）：nack（尝试1，投递回 waiting）→ 滴答越过 3s
# 退避 → 再取拿尝试2 → 再 nack 即死信。全程 holder 租约不被回收。
# 恢复阶段用受控时钟精确模拟“原租约已到期/仍有效”两种现场。
POLICY = dict(lease_seconds=120, retry_seconds=3, max_attempts=2, retention_seconds=3600)

HERE = Path(__file__).resolve().parent
BACKUP_TOOL = HERE / "quaybell_backup.py"
RESTORE_TOOL = HERE / "quaybell_restore.py"
ACCEPT_TOOL = HERE / "quaybell_acceptance.py"


class Api:
    def __init__(self, base):
        self.base = base

    def call(self, method, path, payload=None):
        import urllib.error
        import urllib.request
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.code, json.load(response)
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, json.load(exc)

    def post(self, path, payload):
        return self.call("POST", path, payload)

    def get(self, path):
        return self.call("GET", path)


def run_tool(tool, *args):
    proc = subprocess.run([sys.executable, "-I", str(tool), *map(str, args)],
                          cwd=REPO_ROOT, text=True, capture_output=True, check=False)
    if proc.returncode not in (0, 2, 3):
        raise RuntimeError(f"工具失败 {tool.name} rc={proc.returncode}\n{proc.stderr}")
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def publish(api, key, lane):
    payload = dict(producer="drill-source", key=key, lane=lane,
                   tags={}, payload={"seq": key})
    code, body = api.post(f"/channels/{CHANNEL}/messages", payload)
    assert code == 201, body
    return body


def build_live_state(workdir: Path, scenario: str):
    """构造事故现场，返回 (live_db 路径, 事实文件目录, 时钟, server, thread)。"""
    scenario_dir = workdir / scenario
    live_dir = scenario_dir / "live"
    facts_dir = scenario_dir / "facts"
    backup_dir = scenario_dir / "backups"
    for directory in (live_dir, facts_dir, backup_dir):
        directory.mkdir(parents=True, exist_ok=True)

    clock = [1_760_000_000.0]
    db_path = live_dir / "quaybell.sqlite3"
    persistence = RelayPersistence(db_path)
    server = RelayServer(("127.0.0.1", 0), persistence, lambda: clock[0])
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
    thread.start()
    api = Api(f"http://127.0.0.1:{server.server_port}/v1")
    try:
        api.post("/channels", {"name": CHANNEL, "policy": POLICY})
        api.post(f"/channels/{CHANNEL}/groups", {"name": GROUP, "tags": {}})

        # 事故主角：alpha-lane 上的消息，两次 nack 后进入死信
        alpha = publish(api, "alpha-key", "alpha-lane")
        # 在手租约的承载者：另一条 lane 上的消息，claim 后保持 leased 不再处置
        holder = publish(api, "holder-key", "holder-lane")
        if scenario == "b-same-lane-successor":
            # 同键（同 lane）后继场景：alpha-lane 再放一条，之后也要死信并重投
            beta = publish(api, "beta-key", "alpha-lane")
        else:
            beta = None

        alpha_delivery = alpha["fanout"][GROUP]
        holder_delivery = holder["fanout"][GROUP]
        beta_delivery = beta["fanout"][GROUP] if beta else None

        # 首次取件：alpha（alpha-lane 队头；b 场景下 beta 被它挡住）与 holder 各自成队头。
        # holder 的租约从此保持 leased，取件事实（含 token）即放行方留存的那份。
        _, first_claim = api.post(GROUP_PATH + "/claim",
                                  {"worker": "drill-worker", "limit": 10})
        tokens = {item["delivery_id"]: item["lease"]["token"]
                  for item in first_claim["items"]}
        lease_fact = next(item for item in first_claim["items"]
                          if item["delivery_id"] == holder_delivery)
        (facts_dir / "lease-fact.json").write_text(
            json.dumps(lease_fact, ensure_ascii=False, indent=2), encoding="utf-8")

        def exhaust_to_dead(delivery_id, first_token):
            """尝试1 nack（回 waiting）→ 滴答越过 3s 退避 → 再取拿尝试2 → nack 即死信。"""
            code, _ = api.post(f"/deliveries/{delivery_id}/nack",
                               {"token": first_token, "reason": "演练：下游校验拒绝（尝试1）"})
            assert code == 200
            clock[0] += 4  # 越过 nack 退避(3s)；距 holder 的 120s 租约截止还很远
            _, reclaim = api.post(GROUP_PATH + "/claim",
                                  {"worker": "drill-worker", "limit": 10})
            second_token = next(item["lease"]["token"] for item in reclaim["items"]
                                if item["delivery_id"] == delivery_id)
            code, _ = api.post(f"/deliveries/{delivery_id}/nack",
                               {"token": second_token, "reason": "演练：下游校验拒绝（尝试2）"})
            assert code == 200

        exhaust_to_dead(alpha_delivery, tokens[alpha_delivery])
        if beta_delivery is not None:
            # alpha 死后同 lane 的 beta 解挡，这时才能取到它
            _, beta_claim = api.post(GROUP_PATH + "/claim",
                                     {"worker": "drill-worker", "limit": 10})
            beta_first = next(item["lease"]["token"] for item in beta_claim["items"]
                              if item["delivery_id"] == beta_delivery)
            exhaust_to_dead(beta_delivery, beta_first)

        # 备份一：死信已重投之前（消息条数/最大消息号与正确备份相同）
        run_tool(BACKUP_TOOL, "--db", db_path, "--out", backup_dir,
                 "--label", "backup-early", "--note", "重投之前的错误冷备")

        # 事故处置：死信重投，后继追加到同 lane 队尾，尚未处置
        code, redrive_receipt = api.post(f"/deliveries/{alpha_delivery}/redrive", {})
        assert code == 201, redrive_receipt
        assert redrive_receipt["source_id"] == alpha_delivery
        (facts_dir / "redrive-receipt.json").write_text(
            json.dumps(redrive_receipt, ensure_ascii=False, indent=2), encoding="utf-8")
        if beta_delivery is not None:
            code, beta_receipt = api.post(f"/deliveries/{beta_delivery}/redrive", {})
            assert code == 201, beta_receipt
            (facts_dir / "redrive-receipt-beta.json").write_text(
                json.dumps(beta_receipt, ensure_ascii=False, indent=2), encoding="utf-8")

        # 备份二：死信已重投、后继尚未处置之时（正确冷备）
        run_tool(BACKUP_TOOL, "--db", db_path, "--out", backup_dir,
                 "--label", "backup-correct", "--note", "重投之后、后继未处置的正确冷备")

        # 题设约束：两份备份来自同一频道，消息条数与最大消息号必须相同
        early_manifest = json.loads((backup_dir / "backup-early.manifest.json").read_text())
        correct_manifest = json.loads((backup_dir / "backup-correct.manifest.json").read_text())
        assert early_manifest["summary"]["messages"] == correct_manifest["summary"]["messages"]
        # 投递计数必然不同（重投追加了后继），这正是谱系核对的切入点
        assert (early_manifest["summary"]["deliveries"]["count"]
                < correct_manifest["summary"]["deliveries"]["count"])

        return {"db_path": db_path, "facts": facts_dir, "backups": backup_dir,
                "clock": clock, "server": server, "thread": thread,
                "redrive_receipt": redrive_receipt, "lease_fact": lease_fact,
                "alpha_delivery": alpha_delivery, "holder_delivery": holder_delivery}
    except BaseException:
        server.shutdown()
        thread.join()
        raise


def start_restored(workdir: Path, scenario: str, case: str, label: str, clock_start: float):
    """把指定备份恢复到全新空目录并在新端口起服务，返回 (api, 时钟, server, thread, 目录)。"""
    backup_dir = workdir / scenario / "backups"
    target = workdir / scenario / "restored" / case
    if target.exists():
        # 可重跑：清掉上次的恢复产物，保证每次都是全新目录
        import shutil
        shutil.rmtree(target)
    run_tool(RESTORE_TOOL,
             "--backup", backup_dir / f"{label}.sqlite3",
             "--manifest", backup_dir / f"{label}.manifest.json",
             "--target", target)
    clock = [clock_start]
    persistence = RelayPersistence(target / "quaybell.sqlite3")
    server = RelayServer(("127.0.0.1", 0), persistence, lambda: clock[0])
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
    thread.start()
    api = Api(f"http://127.0.0.1:{server.server_port}/v1")
    return api, clock, server, thread, target


def run_case(workdir: Path, scenario: str, case: str, label: str, clock_start: float,
             clock_tick_before_claim: float):
    facts = workdir / scenario / "facts"
    api, clock, server, thread, target = start_restored(
        workdir, scenario, case, label, clock_start)
    try:
        # 现场流逝发生在任何取件之前：case-2 借此让原租约到期，case-3 不推进
        clock[0] += clock_tick_before_claim
        evidence_dir = target / "acceptance-evidence"
        rc, out, err = run_tool(
            ACCEPT_TOOL,
            "--base", f"http://127.0.0.1:{server.server_port}/v1",
            "--channel", CHANNEL, "--group", GROUP,
            "--redrive-receipt", facts / "redrive-receipt.json",
            "--lease-fact", facts / "lease-fact.json",
            "--evidence-dir", evidence_dir)
        verdict = json.loads((evidence_dir / "99-verdict.json").read_text(encoding="utf-8"))
        return {"case": case, "backup_label": label, "exit_code": rc,
                "verdict": verdict["verdict"], "reason": verdict["reason"],
                "evidence_dir": str(evidence_dir), "tool_stderr": err}
    finally:
        server.shutdown()
        thread.join()


def execute_scenario(workdir: Path, scenario: str, results: list):
    state = build_live_state(workdir, scenario)
    server, thread = state["server"], state["thread"]
    lease_expires = float(state["lease_fact"]["lease"]["expires"])
    try:
        # 恢复用的时钟起点与现场冻结时钟一致；到期与否由取件前的 tick 决定
        base_clock = state["clock"][0]
        # 案例 1：选错备份（重投前快照），无论时钟如何都应被谱系核对拦下
        results.append({"scenario": scenario, **run_case(
            workdir, scenario, "case-1-wrong-backup", "backup-early",
            base_clock, clock_tick_before_claim=0.0)})
        # 案例 2：正确备份 + 取件前观察时租约仍有效，随后时钟越过截止，
        # 由验收本次 claim 的 reap 把 holder 从 leased 推进为 waiting
        results.append({"scenario": scenario, **run_case(
            workdir, scenario, "case-2-correct-but-reaped", "backup-correct",
            lease_expires - 5.0, clock_tick_before_claim=10.0)})
        # 案例 3：正确备份 + 取件前租约仍有效 -> 干净放行
        results.append({"scenario": scenario, **run_case(
            workdir, scenario, "case-3-correct-clean-release", "backup-correct",
            base_clock, clock_tick_before_claim=0.0)})
    finally:
        server.shutdown()
        thread.join()


def main():
    parser = argparse.ArgumentParser(description="Quaybell 冷备放行演练")
    parser.add_argument("--workdir", type=Path, default=HERE / "runtime")
    parser.add_argument("--keep", action="store_true",
                        help="保留并复用 workdir（默认每次重跑先整体清空，保证幂等）")
    args = parser.parse_args()
    workdir = args.workdir
    if workdir.exists() and not args.keep:
        import shutil
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    results = []
    for scenario in ("a-single-lane", "b-same-lane-successor"):
        execute_scenario(workdir, scenario, results)

    summary_path = workdir / "drill-summary.json"
    summary_path.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8")
    for row in results:
        print(f"[{row['scenario']}] {row['case']}: {row['verdict']} (rc={row['exit_code']})")
    expected = ["reject_wrong_backup", "hold_reaped_after_restore", "release_clean_restore"]
    for scenario in ("a-single-lane", "b-same-lane-successor"):
        got = [r["verdict"] for r in results if r["scenario"] == scenario]
        assert got == expected, (scenario, got)
    print(f"\n全部判定符合预期，汇总: {summary_path}")


if __name__ == "__main__":
    main()
