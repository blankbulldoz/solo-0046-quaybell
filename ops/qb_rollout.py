#!/usr/bin/env python3
"""Quaybell 滚动上线工具：共库绑定、新鲜栅栏、旧库复制与误连核对。

仅使用标准库；所有写操作都落在操作员显式给出的 ``--db`` 上。典型顺序：

    # 1) 为共同库建立一次集群绑定
    python3 ops/qb_rollout.py init-cluster --db /srv/qb/common.sqlite3 --cluster prod-a

    # 2) 替换某一端之前，让仍在接活的另一端提交一个“栅栏事实”
    python3 ops/qb_rollout.py fence --db /srv/qb/common.sqlite3 \\
        --via http://127.0.0.1:18092 --channel rollout-proof \\
        --out /srv/qb/fence-18091.json

    # 3) 带栅栏启动替换端（路径钉 + 集群 + 新鲜事实，任一不过即退出）
    QUAYBELL_RELEASE=candidate-r053 python3 -m quaybell --port 18091 \\
        --db /srv/qb/common.sqlite3 --common-db /srv/qb/common.sqlite3 \\
        --expect-cluster prod-a --require-fence /srv/qb/fence-18091.json

    # 验收/排障：对任何候选库跑同一份核对并输出 JSON
    python3 ops/qb_rollout.py verify --db /srv/qb/stale-copy.sqlite3 \\
        --common-db /srv/qb/common.sqlite3 --require-fence /srv/qb/fence-18091.json
"""

import argparse
import hashlib
import json
import secrets
import sqlite3
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from quaybell import rollout  # noqa: E402


def http_post_json(url, payload, timeout=8):
    req = Request(url, data=json.dumps(payload).encode('utf-8'),
                  headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8'))
    except Exception as exc:  # 让操作员看到明确的失败来源
        raise SystemExit(f'通过运行端提交栅栏事实失败 {url}: {exc}') from None


def cmd_init_cluster(args):
    row = rollout.init_cluster(args.db, args.cluster)
    print(json.dumps({'ok': True, 'cluster': row['cluster'],
                      'canonical_realpath': row['canonical_realpath']},
                     ensure_ascii=False, indent=2))


def cmd_fence(args):
    nonce = secrets.token_hex(12)
    key = f'rollout-fence-{secrets.token_hex(6)}'
    envelope = {
        'producer': 'rollout-guard',
        'key': key,
        'lane': 'rollout-fence',
        'tags': {'purpose': 'cutover-fence'},
        'payload': {'nonce': nonce, 'note': args.note or 'rolling replacement fence', 'at': time.time()},
    }
    status, body = http_post_json(args.via.rstrip('/') + f'/v1/channels/{args.channel}/messages', envelope)
    if status not in (200, 201) or 'message_id' not in body:
        raise SystemExit(f'栅栏消息发布异常 status={status} body={body}')
    spec = rollout.record_fence(
        args.db, cluster=args.cluster or _read_cluster(args.db), nonce=nonce,
        channel=args.channel, message_id=body['message_id'], committed_via=args.via, note=args.note)
    out = rollout.write_spec(spec, args.out)
    print(json.dumps({'ok': True, 'http_status': status, 'spec_path': str(out),
                      'fence_id': spec['fence_id'], 'message_id': spec['message_id'],
                      'journal_watermark': spec['journal_watermark']}, ensure_ascii=False, indent=2))


def _read_cluster(path):
    db = sqlite3.connect(str(path), timeout=4)
    try:
        row = db.execute('SELECT cluster FROM rollout_binding WHERE id=1').fetchone()
    except sqlite3.DatabaseError:
        row = None
    finally:
        db.close()
    if row is None:
        raise SystemExit('共同库尚未 init-cluster，且未显式提供 --cluster')
    return row[0]


def cmd_decoy_copy(args):
    """用 SQLite backup API 做一致性复制（即使源库处于 WAL 且有进程在写）。"""
    source = rollout.real_path(args.source)
    target = rollout.real_path(args.target)
    if not source.is_file():
        raise SystemExit(f'源库不存在: {source}')
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    src = sqlite3.connect(str(source), timeout=10)
    dst = sqlite3.connect(str(target), timeout=10)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    print(json.dumps({'ok': True, 'source': str(source), 'target': str(target),
                      'bytes': target.stat().st_size, 'sha256': digest},
                     ensure_ascii=False, indent=2))


def cmd_verify(args):
    spec = None
    if args.require_fence:
        try:
            spec = rollout.load_spec(args.require_fence)
        except rollout.FenceFileError as exc:
            print(json.dumps({'ok': False, 'code': 'BAD_FENCE', 'message': str(exc)},
                             ensure_ascii=False, indent=2))
            return rollout.EXIT_BAD_FENCE
    report = rollout.evaluate(args.db, common_db=args.common_db,
                              expected_cluster=args.expect_cluster, spec=spec)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report['exit_code']


def main():
    parser = argparse.ArgumentParser(description='Quaybell 滚动上线防误连工具')
    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('init-cluster', help='在共同库写入唯一集群绑定')
    p.add_argument('--db', required=True, type=Path)
    p.add_argument('--cluster', required=True)
    p.set_defaults(func=cmd_init_cluster)

    p = sub.add_parser('fence', help='经运行端提交栅栏消息并在共同库记录新鲜事实')
    p.add_argument('--db', required=True, type=Path)
    p.add_argument('--via', required=True, help='仍在接活的伙伴端 HTTP 基址，如 http://127.0.0.1:18092')
    p.add_argument('--channel', required=True, help='栅栏消息频道（需已存在，建议专用空分组频道）')
    p.add_argument('--out', required=True, type=Path)
    p.add_argument('--cluster', default=None)
    p.add_argument('--note', default=None)
    p.set_defaults(func=cmd_fence)

    p = sub.add_parser('decoy-copy', help='一致性复制一份旧库（验收诱饵/排障副本）')
    p.add_argument('--source', required=True, type=Path)
    p.add_argument('--target', required=True, type=Path)
    p.set_defaults(func=cmd_decoy_copy)

    p = sub.add_parser('verify', help='按上线同一套核对检查候选库')
    p.add_argument('--db', required=True, type=Path)
    p.add_argument('--common-db', type=Path, default=None)
    p.add_argument('--expect-cluster', default=None)
    p.add_argument('--require-fence', type=Path, default=None)
    p.set_defaults(func=cmd_verify)

    args = parser.parse_args()
    result = args.func(args)
    if args.command == 'verify':
        sys.exit(result or 0)


if __name__ == '__main__':
    main()
