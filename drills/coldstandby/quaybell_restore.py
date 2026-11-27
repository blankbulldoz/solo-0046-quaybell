#!/usr/bin/env python3
"""Quaybell 冷备恢复工具：把备份快照恢复到一个全新的空目录。

仅依赖 Python 标准库，只处理受信的冷备文件；恢复目标目录必须不存在或为空，
避免覆盖现场。恢复后校验校验和、schema 版本与完整性，并把恢复出的库切成
DELETE 日志模式，保证目录里只有一个自包含的 sqlite 文件。

用法:
  python3 -I quaybell_restore.py --backup <备份目录/标签.sqlite3> \\
      --manifest <备份目录/标签.manifest.json> --target <新恢复目录>
"""
import argparse
import hashlib
import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_NAME = "quaybell.sqlite3"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Quaybell 冷备恢复到新目录")
    parser.add_argument("--backup", required=True, type=Path, help="备份快照 .sqlite3")
    parser.add_argument("--manifest", required=True, type=Path, help="备份清单 .manifest.json")
    parser.add_argument("--target", required=True, type=Path, help="恢复目标目录（必须为空）")
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    expected = manifest["sha256"]
    actual = sha256_file(args.backup)
    if actual != expected:
        raise SystemExit(f"备份校验和不匹配，拒绝恢复: 期望 {expected} 实际 {actual}")

    if args.target.exists() and any(args.target.iterdir()):
        raise SystemExit(f"恢复目标目录非空，拒绝覆盖: {args.target}")
    args.target.mkdir(parents=True, exist_ok=True)
    restored = args.target / DB_NAME
    shutil.copy2(args.backup, restored)

    db = sqlite3.connect(restored)
    try:
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version != manifest["summary"]["schema_version"]:
            raise SystemExit("恢复库 schema 版本与清单不一致")
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db.execute("PRAGMA journal_mode=DELETE")
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise SystemExit(f"恢复库完整性检查失败: {integrity}")
        quickcheck = db.execute("PRAGMA quick_check").fetchone()[0]
        if quickcheck != "ok":
            raise SystemExit(f"恢复库快速检查失败: {quickcheck}")
    finally:
        db.close()

    report = {
        "format": "quaybell-restore/1",
        "restored_at_unix": int(datetime.now(tz=timezone.utc).timestamp()),
        "source_backup": str(args.backup),
        "source_label": manifest.get("label"),
        "restored_db": str(restored),
        "sha256_verified": actual,
        "schema_version": version,
        "integrity_check": integrity,
        "quick_check": quickcheck,
        "summary": manifest["summary"],
    }
    report_path = args.target / "restore-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2,
                                      sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
