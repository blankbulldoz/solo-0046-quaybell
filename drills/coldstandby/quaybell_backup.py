#!/usr/bin/env python3
"""Quaybell 冷备工具：对本机 SQLite 状态库做在线一致性备份并生成取证清单。

仅依赖 Python 标准库。被备份的数据库是不受信的现场数据，本脚本只通过
sqlite3 的在线备份 API 读取它，不执行其中任何代码。

用法:
  python3 -I quaybell_backup.py --db <状态库> --out <备份目录> [--label 名称]

产物:
  <备份目录>/<label>.sqlite3        一致性快照（可直接作为独立状态库打开）
  <备份目录>/<label>.manifest.json  消息/投递/尝试的计数与关键状态指纹
  <备份目录>/<label>.sha256         快照文件的 SHA-256
"""
import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SUMMARY_SQL = {
    "messages": "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS max_id FROM messages",
    "deliveries": "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS max_id FROM deliveries",
    "attempts": "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS max_id FROM attempts",
}

STATE_SQL = """
SELECT state, COUNT(*) AS n FROM deliveries GROUP BY state ORDER BY state
"""

OPEN_ATTEMPTS_SQL = """
SELECT a.delivery_id, a.number, a.worker, a.started, a.expires
FROM attempts a WHERE a.outcome='open' ORDER BY a.expires, a.id
"""

DEAD_REDRIVEN_SQL = """
SELECT d.id AS dead_id, c.id AS successor_id, d.message_id, d.lane,
       d.subscription_id, c.state AS successor_state
FROM deliveries d
LEFT JOIN deliveries c ON c.parent_id = d.id
WHERE d.state = 'dead'
ORDER BY d.id
"""

LANE_HEADS_SQL = """
SELECT d.subscription_id, d.lane, MIN(d.id) AS head_id
FROM deliveries d
WHERE d.state IN ('waiting', 'leased')
GROUP BY d.subscription_id, d.lane
ORDER BY d.subscription_id, d.lane
"""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def online_copy(source: Path, target: Path) -> None:
    """通过 sqlite3 在线备份 API 复制 WAL 库，服务进程可同时运行。"""
    target.parent.mkdir(parents=True, exist_ok=True)
    source_db = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    target_db = sqlite3.connect(target)
    try:
        source_db.backup(target_db)
        target_db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        target_db.execute("PRAGMA journal_mode=DELETE")
    finally:
        target_db.close()
        source_db.close()


def summarize(path: Path) -> dict:
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        user_version = db.execute("PRAGMA user_version").fetchone()[0]
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        summary = {"schema_version": user_version, "integrity_check": integrity}
        for name, sql in SUMMARY_SQL.items():
            row = db.execute(sql).fetchone()
            summary[name] = {"count": row["n"], "max_id": row["max_id"]}
        summary["delivery_states"] = {row["state"]: row["n"]
                                      for row in db.execute(STATE_SQL)}
        summary["open_attempts"] = [dict(row) for row in db.execute(OPEN_ATTEMPTS_SQL)]
        summary["dead_and_redriven"] = [dict(row) for row in db.execute(DEAD_REDRIVEN_SQL)]
        summary["active_lane_heads"] = [dict(row) for row in db.execute(LANE_HEADS_SQL)]
    finally:
        db.close()
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Quaybell 一致性冷备")
    parser.add_argument("--db", required=True, type=Path, help="在线状态库路径")
    parser.add_argument("--out", required=True, type=Path, help="备份输出目录（新建）")
    parser.add_argument("--label", required=True, help="备份标签，用于文件名")
    parser.add_argument("--note", default="", help="写入清单的人工备注")
    args = parser.parse_args()

    if not args.db.exists():
        parser.error(f"状态库不存在: {args.db}")
    args.out.mkdir(parents=True, exist_ok=True)
    snapshot = args.out / f"{args.label}.sqlite3"
    manifest_path = args.out / f"{args.label}.manifest.json"
    digest_path = args.out / f"{args.label}.sha256"
    if snapshot.exists() or manifest_path.exists():
        parser.error(f"备份标签已存在，拒绝覆盖: {args.label}")

    online_copy(args.db, snapshot)
    checksum = sha256_file(snapshot)
    manifest = {
        "format": "quaybell-cold-backup/1",
        "label": args.label,
        "note": args.note,
        "taken_at_unix": int(datetime.now(tz=timezone.utc).timestamp()),
        "source_db": str(args.db),
        "snapshot": snapshot.name,
        "sha256": checksum,
        "summary": summarize(snapshot),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2,
                                        sort_keys=True) + "\n", encoding="utf-8")
    digest_path.write_text(f"{checksum}  {snapshot.name}\n", encoding="utf-8")
    print(json.dumps({"label": args.label, "snapshot": str(snapshot),
                      "sha256": checksum,
                      "messages": manifest["summary"]["messages"],
                      "deliveries": manifest["summary"]["deliveries"]},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
