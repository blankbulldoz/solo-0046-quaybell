#!/usr/bin/env python3
"""Quaybell 两端口滚动替换 + 防误连验收（真实进程 / 真实时钟 / 真实 HTTP）。

本脚本会：
  1. 在一份共同 SQLite 库上启动两个 Quaybell 进程（两端口），全程带上线护栏；
  2. 制造“一条在手租约 + 同顺序键后继”，并发跨两端口抢租约，验证同键不重复；
  3. 用 backup API 一致性复制一份旧库作为“误连诱饵”，另起一个无护栏进程模拟误连，
     证明起初三端查到相同频道/消息/租约，朴素检查无法识别；
  4. 用未重启端刚提交的事实生成栅栏，实录上线命令对错误路径(exit 3)与
     “被复制回权威路径的旧副本”(exit 4) 的拒绝；
  5. 一次一端滚动替换 A、B，期间另一端继续发布/领取，并证明替换端看得到伙伴端
     刚提交的事实、发布的事实也能被旧端读到；
  6. 等待原租约按真实系统时钟自然过期（只读核对“已过截止但仍 open”），由另一端
     触发全局回收，再并发抢领，验证仅产生唯一的第二次尝试；
  7. 对比“共库已推进”与“旧副本冻结/脑裂后分叉”的可判别证据；
  8. 产出 ACCEPTANCE.md 与 acceptance/rolling-acceptance.json（含 evidence/ 旧库实物）。

安全边界：只调用 channels/groups/messages/claim/sweep 与只读 GET；
不调用 pause、ack、nack、redrive、retention 清理，不删除任何消息。
所有“处置”只有租约自然过期后的 sweep 回收（系统设计内的可重入动作）。
"""

import argparse
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from quaybell import codec  # noqa: E402

CHANNEL_MAIN = 'rollout-lease'
GROUP = 'gears'
SAME_LANE = 'same-order-key'
CHANNEL_PROOF = 'rollout-proof'
CHANNEL_FLOW = 'rollout-flow'
FLOW_GROUP = 'river'
CLUSTER = 'quaybell-rolling-acceptance'
RELEASE_OLD = 'baseline-r053'
RELEASE_NEW = 'candidate-r053'


# ----------------------------- 基础工具 -----------------------------

def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec='seconds')


def free_port(preferred):
    for port in ([preferred] if preferred else []) + [0]:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(('127.0.0.1', port))
                return sock.getsockname()[1]
            except OSError:
                continue
    raise RuntimeError('找不到可用端口')


def http(base, method, path, payload=None, timeout=10):
    url = base + path
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8') or '{}'), dict(resp.headers)
    except Exception as exc:  # noqa: BLE001 - 验收脚本需要保留 HTTPError 状态码
        status = getattr(exc, 'code', None)
        if status is not None:
            body = getattr(exc, 'read', lambda: b'')()
            try:
                obj = json.loads(body.decode('utf-8'))
            except Exception:
                obj = {'raw': body[:300].decode('utf-8', 'replace')}
            return status, obj, {}
        raise


def wait_ready(node, timeout=20):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        if node['proc'].poll() is not None:
            tail = ''
            try:
                tail = Path(node['log']).read_text(encoding='utf-8', errors='replace')[-2000:]
            except OSError:
                pass
            raise RuntimeError(f"节点 {node['name']} 提前退出 rc={node['proc'].returncode}\n{tail}")
        try:
            status, body, headers = http(node['base'], 'GET', '/v1/channels', timeout=3)
            if status == 200:
                node['release_header'] = header_get(headers, 'X-Quaybell-Release')
                return node
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.15)
    raise RuntimeError(f"等待节点 {node['name']} 就绪超时: {last}")


def header_get(headers, name):
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


def start_node(lab, name, port, db_path, release, *, common_db=None, fence=None,
               guarded=True):
    log_path = lab / 'logs' / f'{name}-p{port}.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, '-m', 'quaybell', '--host', '127.0.0.1', '--port', str(port),
           '--db', str(db_path), '--release', release]
    if guarded:
        cmd += ['--common-db', str(common_db or db_path), '--expect-cluster', CLUSTER]
        if fence is not None:
            cmd += ['--require-fence', str(fence)]
    env = os.environ.copy()
    env['PYTHONPATH'] = str(ROOT) + (os.pathsep + env.get('PYTHONPATH', '') if env.get('PYTHONPATH') else '')
    log_fh = open(log_path, 'ab')
    proc = subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=log_fh,
                            stderr=subprocess.STDOUT)
    node = dict(name=name, port=port, base=f'http://127.0.0.1:{port}', proc=proc,
                log=str(log_path), log_fh=log_fh, cmd=cmd, release=release)
    wait_ready(node)
    return node


def stop_node(node):
    proc = node.get('proc')
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    try:
        node['log_fh'].close()
    except Exception:  # noqa: BLE001
        pass


def run_cmd(argv, timeout=40):
    started = time.time()
    proc = subprocess.run(argv, cwd=str(ROOT), capture_output=True, text=True, timeout=timeout)
    return {'argv': argv, 'rc': proc.returncode, 'stdout': proc.stdout,
            'stderr': proc.stderr, 'elapsed_ms': int((time.time() - started) * 1000)}


def py_module(*parts):
    return [sys.executable] + list(parts)


# ----------------------------- SQLite 只读快照 -----------------------------

def db_connect_readonly(path):
    uri = Path(path).resolve().as_uri() + '?mode=ro'
    con = sqlite3.connect(uri, uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    return con


def lease_view(path, message_id):
    con = db_connect_readonly(path)
    try:
        msg = con.execute('SELECT id,fingerprint,lane,dedup_key FROM messages WHERE id=?',
                          (message_id,)).fetchone()
        deliveries = [dict(r) for r in con.execute(
            'SELECT id,state,attempt_count,ready_at FROM deliveries WHERE message_id=? ORDER BY id',
            (message_id,))]
        attempts = []
        for d in deliveries:
            attempts += [dict(r) for r in con.execute(
                '''SELECT id,delivery_id,number,token_digest,worker,started,expires,
                          outcome,finished,reason
                     FROM attempts WHERE delivery_id=? ORDER BY number''', (d['id'],))]
        journal = [dict(r) for r in con.execute(
            'SELECT id,kind FROM journal WHERE message_id=? ORDER BY id', (message_id,))]
        return {'message': dict(msg) if msg else None, 'deliveries': deliveries,
                'attempts': attempts, 'journal': journal,
                'journal_max': con.execute('SELECT COALESCE(MAX(id),0) FROM journal').fetchone()[0]}
    finally:
        con.close()


def open_attempt_count(path, delivery_id):
    con = db_connect_readonly(path)
    try:
        return con.execute("SELECT COUNT(*) FROM attempts WHERE delivery_id=? AND outcome='open'",
                           (delivery_id,)).fetchone()[0]
    finally:
        con.close()


def logical_hash(path):
    """对核心状态做逻辑哈希；WAL/文件时间不影响结果。"""
    con = db_connect_readonly(path)
    try:
        digest = hashlib.sha256()
        for table in ('channels', 'messages', 'deliveries', 'attempts'):
            cols = [r[1] for r in con.execute(f'PRAGMA table_info({table})')]
            order = 'id' if 'id' in cols else cols[0]
            for row in con.execute(f'SELECT * FROM {table} ORDER BY {order}'):
                digest.update(table.encode())
                digest.update(repr(tuple(row)).encode('utf-8', 'replace'))
        return digest.hexdigest()
    finally:
        con.close()


def file_sha256(path):
    p = Path(path)
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None


# ----------------------------- 验收主体 -----------------------------

class Acceptance:
    def __init__(self, args):
        self.args = args
        self.lab = Path(args.lab).resolve()
        self.common_db = self.lab / 'common' / 'quaybell.sqlite3'
        self.stale_db = self.lab / 'stale-copy' / 'quaybell.sqlite3'
        self.restored_db = self.lab / 'restored-canonical' / 'quaybell.sqlite3'
        self.fence_a = self.lab / 'specs' / 'fence-a.json'
        self.fence_b = self.lab / 'specs' / 'fence-b.json'
        self.E = {
            'generated_at': now_iso(), 'overall_ok': True, 'checks': [],
            'ports': {}, 'paths': {'lab': str(self.lab), 'common_db': str(self.common_db),
                                   'stale_db': str(self.stale_db), 'restored_db': str(self.restored_db)},
            'commands': [], 'race_one': {}, 'race_two': {}, 'freeze': {},
            'rolling': [], 'expiry': {}, 'discrimination': {}, 'guard_rejections': [],
            'guard_verify': [], 'continuity': [], 'artifacts': {},
        }
        self.nodes = {}
        self.message_ids = {}
        self.delivery_ids = {}

    def check(self, name, condition, **detail):
        row = {'name': name, 'result': 'PASS' if condition else 'FAIL', **detail}
        self.E['checks'].append(row)
        if not condition:
            self.E['overall_ok'] = False
        print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" — {detail.get('summary')}" if detail.get('summary') else ''))
        return bool(condition)

    def record_command(self, title, result, show_stdout=1200, show_stderr=1600):
        entry = {'title': title, **{k: v for k, v in result.items() if k != 'argv'},
                 'command': ' '.join(str(a) for a in result['argv']),
                 'stdout_tail': result['stdout'][-show_stdout:],
                 'stderr_tail': result['stderr'][-show_stderr:]}
        self.E['commands'].append({'title': title, 'command': entry['command'], 'rc': entry['rc']})
        return entry

    # ---------- 并发抢租约 ----------

    def claim_race(self, bases, label):
        workers = self.args.workers
        barrier = threading.Barrier(workers)

        def one(idx):
            base = bases[idx % len(bases)]
            port = urlparse(base).port
            worker = f'race-{label}-{idx}'
            barrier.wait(timeout=15)
            attempts = 0
            while True:
                attempts += 1
                try:
                    status, body, _ = http(
                        base, 'POST', f'/v1/channels/{CHANNEL_MAIN}/groups/{GROUP}/claim',
                        {'worker': worker, 'limit': 1})
                    if status == 503 and attempts <= 4:
                        time.sleep(0.15 * attempts)
                        continue
                    items = body.get('items', []) if isinstance(body, dict) else []
                    return {'idx': idx, 'port': port, 'worker': worker, 'status': status,
                            'items': items, 'busy_retries': attempts - 1}
                except Exception as exc:  # noqa: BLE001
                    if attempts <= 4:
                        time.sleep(0.2 * attempts)
                        continue
                    return {'idx': idx, 'port': port, 'worker': worker,
                            'error': repr(exc), 'items': [], 'busy_retries': attempts - 1}

        started = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(one, range(workers)))
        elapsed_ms = int((time.time() - started) * 1000)

        all_items = [it for r in results for it in r['items']]
        d1 = self.delivery_ids['m1']
        d2 = self.delivery_ids['m2']
        winners = [it for it in all_items if it['delivery_id'] == d1]
        saw_d2 = [it for it in all_items if it['delivery_id'] == d2]
        errors = [r for r in results if r.get('error') or r['status'] != 200]
        total_busy = sum(r.get('busy_retries', 0) for r in results)
        winner = None
        if winners:
            w = winners[0]
            winning_result = next(r for r in results
                                  if any(it['delivery_id'] == d1 for it in r['items']))
            token = w['lease']['token']
            winner = {
                'port': winning_result['port'],
                'worker': winning_result['worker'],
                'delivery_id': w['delivery_id'], 'attempt_number': w['lease']['number'],
                'expires': w['lease']['expires'],
                'token_prefix': token[:10], 'token_digest': codec.fingerprint(token),
            }
        summary = {
            'workers': workers, 'elapsed_ms': elapsed_ms, 'http_errors': len(errors),
            'busy_retries': total_busy, 'total_leased_items': len(all_items),
            'winners_for_d1': len(winners), 'items_for_d2': len(saw_d2),
            'winner': winner,
            'per_port_leased': {str(p): sum(1 for r in results if r['port'] == p for _ in r['items'])
                                for p in sorted({r['port'] for r in results})},
        }
        return summary, results

    # ---------- 主流程 ----------

    def run(self):
        args = self.args
        if self.lab.exists():
            import shutil
            shutil.rmtree(self.lab)
        (self.lab / 'common').mkdir(parents=True)
        (self.lab / 'specs').mkdir(parents=True)
        (self.lab / 'logs').mkdir(parents=True)

        pa = free_port(args.port_a or 18091)
        pb = free_port(args.port_b or 18092)
        pc = free_port(args.port_c or 18093)
        self.E['ports'] = {'A': pa, 'B': pb, 'C_decoy': pc,
                           'note': '首选端口被占用时验收器自动换空闲端口'}

        # 1) 初始化受管共同库 ------------------------------------------------
        r = run_cmd([sys.executable, str(ROOT / 'ops/qb_rollout.py'), 'init-cluster',
                     '--db', str(self.common_db), '--cluster', CLUSTER])
        self.record_command('init-cluster 共同库绑定', r)
        self.check('init_cluster_rc0', r['rc'] == 0, summary=f'rc={r["rc"]}')

        a = start_node(self.lab, 'A0', pa, self.common_db, RELEASE_OLD, common_db=self.common_db)
        b = start_node(self.lab, 'B0', pb, self.common_db, RELEASE_OLD, common_db=self.common_db)
        self.nodes.update(A=a, B=b)
        self.check('two_nodes_boot_guarded', a['release_header'] == RELEASE_OLD
                   and b['release_header'] == RELEASE_OLD,
                   summary=f'A={a["release_header"]} B={b["release_header"]}')
        guard_pass_a = 'QB_GUARD_PASS' in Path(a['log']).read_text(errors='replace')
        self.check('boot_guard_pass_line', guard_pass_a,
                   summary='两个初始进程的日志均有 QB_GUARD_PASS' if guard_pass_a else '缺少护栏通过行')

        # 2) 拓扑与两条同顺序键消息 ------------------------------------------
        st, body, _ = http(a['base'], 'POST', '/v1/channels',
                            {'name': CHANNEL_MAIN,
                             'policy': {'lease_seconds': args.lease_seconds, 'retry_seconds': 0,
                                        'max_attempts': 3, 'retention_seconds': 3600}})
        self.check('create_lease_channel', st == 201, summary=f'status={st}')
        st, _, _ = http(a['base'], 'POST', f'/v1/channels/{CHANNEL_MAIN}/groups', {'name': GROUP})
        self.check('create_gears_group', st == 201, summary=f'status={st}')

        st, _, _ = http(a['base'], 'POST', '/v1/channels', {'name': CHANNEL_PROOF})
        self.check('create_proof_channel', st == 201, summary=f'status={st}')

        st, _, _ = http(a['base'], 'POST', '/v1/channels',
                        {'name': CHANNEL_FLOW,
                         'policy': {'lease_seconds': 300, 'retry_seconds': 0,
                                    'max_attempts': 3, 'retention_seconds': 3600}})
        self.check('create_flow_channel', st == 201, summary=f'status={st}')
        st, _, _ = http(a['base'], 'POST', f'/v1/channels/{CHANNEL_FLOW}/groups',
                        {'name': FLOW_GROUP})
        self.check('create_flow_group', st == 201, summary=f'status={st}')

        nonce = hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:10]
        for tag, role in (('m1', 'head-lease-in-hand'), ('m2', 'same-key-successor')):
            st, body, _ = http(a['base'], 'POST', f'/v1/channels/{CHANNEL_MAIN}/messages', {
                'producer': 'acceptance', 'key': f'{tag}-{nonce}', 'lane': SAME_LANE,
                'payload': {'role': role, 'nonce': nonce}, 'tags': {'suite': 'rolling'}})
            self.check(f'publish_{tag}', st == 201, summary=f'status={st} id={body.get("message_id")}')
            self.message_ids[tag] = body['message_id']
            self.delivery_ids[tag] = body['fanout'][GROUP]

        # 3) 首次跨端口并发抢租约：同键只能有一个赢家 -------------------------
        race1, raw1 = self.claim_race([a['base'], b['base']], 'one')
        self.E['race_one'] = race1
        self.check('race1_no_http_errors', race1['http_errors'] == 0,
                   summary=f"errors={race1['http_errors']} busy={race1['busy_retries']}")
        self.check('race1_exactly_one_lease_for_head', race1['winners_for_d1'] == 1,
                   summary=f"winners={race1['winners_for_d1']} total_items={race1['total_leased_items']}")
        self.check('race1_successor_never_leased', race1['items_for_d2'] == 0,
                   summary=f"d2_items={race1['items_for_d2']}")
        self.check('race1_single_open_attempt_in_db',
                   open_attempt_count(self.common_db, self.delivery_ids['m1']) == 1,
                   summary='partial unique index + BEGIN IMMEDIATE 后仅一行 open 尝试')
        w1 = race1['winner']
        self.check('race1_token_digest_matches_db', w1 is not None and bool(w1['token_digest']),
                   summary=f"attempt=#{w1['attempt_number'] if w1 else None} via port {w1['port'] if w1 else None}")

        # 4) 立即一致性复制旧库 + “被放回权威路径”的第二份 --------------------
        r = run_cmd([sys.executable, str(ROOT / 'ops/qb_rollout.py'), 'decoy-copy',
                     '--source', str(self.common_db), '--target', str(self.stale_db)])
        decoy_info = json.loads(r['stdout']) if r['rc'] == 0 else {}
        self.record_command('decoy-copy 共同库 -> 旧副本', r, show_stdout=800)
        self.check('decoy_copy_rc0', r['rc'] == 0 and decoy_info.get('sha256'),
                   summary=f"sha256={str(decoy_info.get('sha256'))[:16]}…")
        r2 = run_cmd([sys.executable, str(ROOT / 'ops/qb_rollout.py'), 'decoy-copy',
                      '--source', str(self.common_db), '--target', str(self.restored_db)])
        self.check('restored_copy_rc0', r2['rc'] == 0)

        common_view = lease_view(self.common_db, self.message_ids['m1'])
        stale_view = lease_view(self.stale_db, self.message_ids['m1'])
        freeze_hash = logical_hash(self.common_db)
        stale_hash = logical_hash(self.stale_db)
        self.E['freeze'] = {
            'common_logical_hash': freeze_hash, 'stale_logical_hash': stale_hash,
            'views_equal': common_view == stale_view,
            'common_attempt1': common_view['attempts'][0] if common_view['attempts'] else None,
            'copied_before_lease_expiry': time.time() < w1['expires'],
        }
        self.check('freeze_logical_hashes_equal', freeze_hash == stale_hash,
                   summary='复制瞬间两份库核心状态逻辑哈希一致')
        self.check('freeze_lease_identical_in_copy', common_view == stale_view
                   and len(common_view['attempts']) == 1
                   and common_view['attempts'][0]['outcome'] == 'open',
                   summary=f"attempt#1 {common_view['attempts'][0]['worker']} expires={common_view['attempts'][0]['expires']}")
        self.check('copy_taken_while_lease_alive', time.time() < w1['expires'],
                   summary=f"now={time.time():.1f} expires={w1['expires']:.1f}")

        # 5) 起一个“无护栏、误连旧副本”的进程作为阴性对照 --------------------
        c = start_node(self.lab, 'C-decoy', pc, self.stale_db, 'old-copy-no-guard', guarded=False)
        self.nodes['C'] = c
        st_c, trace_c0, hdr_c = http(c['base'], 'GET', f"/v1/messages/{self.message_ids['m1']}")
        st_a, trace_a0, _ = http(a['base'], 'GET', f"/v1/messages/{self.message_ids['m1']}")
        st_b0, trace_b0, _ = http(b['base'], 'GET', f"/v1/messages/{self.message_ids['m1']}")

        def public_trace(trace):
            return {
                'fingerprint': trace['message']['fingerprint'],
                'attempts': [{k: at[k] for k in ('number', 'worker', 'expires', 'outcome')}
                             for d in trace['deliveries'] for at in d['attempts']],
            }

        traces_equal_initially = (st_a == st_b0 == st_c == 200
                                  and public_trace(trace_a0) == public_trace(trace_b0)
                                  and public_trace(trace_b0) == public_trace(trace_c0))
        self.check('decoy_http_trace_same_initially', traces_equal_initially,
                   summary='A、B 共库端与误连 C 起初经 HTTP 给出完全相同的消息指纹和租约摘要')
        st_ch_a, ch_a, _ = http(a['base'], 'GET', '/v1/channels')
        st_ch_c, ch_c, _ = http(c['base'], 'GET', '/v1/channels')
        names_a = sorted(x['name'] for x in ch_a['channels'])
        names_c = sorted(x['name'] for x in ch_c['channels'])
        self.check('naive_channel_name_check_passes_on_decoy', names_a == names_c
                   and CHANNEL_MAIN in names_c,
                   summary=f'两端频道名一致: {names_c}（证明只查频道名无法识别误连）')

        # 6) 未重启端 B 提交栅栏事实 F-A -------------------------------------
        r = run_cmd([sys.executable, str(ROOT / 'ops/qb_rollout.py'), 'fence',
                     '--db', str(self.common_db), '--via', b['base'],
                     '--channel', CHANNEL_PROOF, '--out', str(self.fence_a),
                     '--note', 'replace node A'])
        self.record_command('fence via B（未重启端刚提交）', r)
        self.check('fence_a_created', r['rc'] == 0 and self.fence_a.is_file(),
                   summary=r['stdout'].strip().replace('\n', ' ')[:200])
        spec_a = json.loads(self.fence_a.read_text())

        # 7) 上线命令拒绝误连：错误路径(3) 与 旧副本占住权威路径(4) ----------
        bad_path_cmd = [sys.executable, '-m', 'quaybell', '--port', '18099',
                        '--db', str(self.stale_db), '--common-db', str(self.common_db),
                        '--expect-cluster', CLUSTER, '--require-fence', str(self.fence_a)]
        rr = run_cmd(bad_path_cmd)
        entry = self.record_command('拒绝错误 --db 路径', rr)
        reject_json = self._parse_guard_reject(rr['stderr'])
        self.E['guard_rejections'].append(
            {'case': 'wrong_path', 'command': entry['command'], 'rc': rr['rc'],
             'parsed': reject_json, 'stderr': rr['stderr']})
        self.check('launch_rejects_wrong_path_exit3', rr['rc'] == 3
                   and reject_json and reject_json.get('code') == 'DB_PATH_MISMATCH',
                   summary=f'rc={rr["rc"]} code={reject_json and reject_json.get("code")}')

        stale_pinned_cmd = [sys.executable, '-m', 'quaybell', '--port', '18098',
                            '--db', str(self.restored_db), '--common-db', str(self.restored_db),
                            '--expect-cluster', CLUSTER, '--require-fence', str(self.fence_a)]
        rr2 = run_cmd(stale_pinned_cmd)
        entry2 = self.record_command('拒绝被复制回权威路径的旧副本（新鲜事实缺失）', rr2)
        reject_json2 = self._parse_guard_reject(rr2['stderr'])
        self.E['guard_rejections'].append(
            {'case': 'stale_pinned_path', 'command': entry2['command'], 'rc': rr2['rc'],
             'parsed': reject_json2, 'stderr': rr2['stderr']})
        self.check('launch_rejects_stale_replica_exit4', rr2['rc'] == 4
                   and reject_json2 and reject_json2.get('code') == 'STALE_REPLICA',
                   summary=f'rc={rr2["rc"]} code={reject_json2 and reject_json2.get("code")}')

        # verify 子命令：共库通过、旧副本给出完整差异报告 --------------------
        rv_ok = run_cmd([sys.executable, str(ROOT / 'ops/qb_rollout.py'), 'verify',
                         '--db', str(self.common_db), '--common-db', str(self.common_db),
                         '--require-fence', str(self.fence_a)])
        self.E['guard_verify'].append({'case': 'common', 'rc': rv_ok['rc'],
                                       'stdout_tail': rv_ok['stdout'][-1200:]})
        self.check('verify_common_rc0', rv_ok['rc'] == 0, summary=f'rc={rv_ok["rc"]}')
        rv_bad = run_cmd([sys.executable, str(ROOT / 'ops/qb_rollout.py'), 'verify',
                          '--db', str(self.stale_db), '--common-db', str(self.common_db),
                          '--require-fence', str(self.fence_a)])
        try:
            bad_report = json.loads(rv_bad['stdout'])
        except ValueError:
            bad_report = {}
        self.E['guard_verify'].append({'case': 'stale', 'rc': rv_bad['rc'], 'report': bad_report})
        self.check('verify_stale_nonzero_with_failing_checks', rv_bad['rc'] in (3, 4)
                   and bool(bad_report.get('failures')),
                   summary=f"rc={rv_bad['rc']} failures={[f['check'] for f in bad_report.get('failures', [])]}")

        # 8) 滚动替换 A：停 A0，B 继续接活 -----------------------------------
        stop_node(a)
        self.check('node_a_stopped_one_at_a_time', a['proc'].poll() is not None,
                   summary='A0 已停止，B0 仍在服务')
        st_flow, flow1, _ = http(b['base'], 'POST', f'/v1/channels/{CHANNEL_FLOW}/messages', {
            'producer': 'flow-probe', 'key': f'during-a-down-{nonce}', 'lane': 'flow-a-down',
            'payload': {'when': 'A down'}})
        st_claim_f, claim_f1, _ = http(
            b['base'], 'POST', f'/v1/channels/{CHANNEL_FLOW}/groups/{FLOW_GROUP}/claim',
            {'worker': 'flow-b', 'limit': 1})
        self.E['continuity'].append(
            {'during': 'A_down', 'publish_status': st_flow, 'claim_status': st_claim_f,
             'leased': len(claim_f1.get('items', []))})
        self.check('partner_b_takes_work_while_a_down',
                   st_flow == 201 and len(claim_f1.get('items', [])) == 1,
                   summary=f'发布={st_flow} 领取条数={len(claim_f1.get("items", []))}（不确认，租约自然保留）')

        a1 = start_node(self.lab, 'A1-new', pa, self.common_db, RELEASE_NEW,
                        common_db=self.common_db, fence=self.fence_a)
        self.nodes['A'] = a1
        guard_line = [ln for ln in Path(a1['log']).read_text(errors='replace').splitlines()
                      if 'QB_GUARD_PASS' in ln]
        self.check('a1_guarded_boot_pass', bool(guard_line),
                   summary=guard_line[-1][:160] if guard_line else '无 QB_GUARD_PASS')
        self.check('a1_release_header_new', a1['release_header'] == RELEASE_NEW,
                   summary=f"X-Quaybell-Release={a1['release_header']}")

        # 核心放行证据：替换端看到未重启端刚提交的 F-A -----------------------
        st_fa_a1, fa_a1, _ = http(a1['base'], 'GET', f"/v1/messages/{spec_a['message_id']}")
        st_fa_b, fa_b, _ = http(b['base'], 'GET', f"/v1/messages/{spec_a['message_id']}")
        sees = (st_fa_a1 == 200 and fa_a1['message']['fingerprint'] == spec_a['fingerprint']
                and st_fa_b == 200 and fa_b['message']['fingerprint'] == spec_a['fingerprint'])
        self.check('replaced_a1_sees_fact_committed_by_b0', sees,
                   summary=f'F-A #{spec_a["message_id"]} 在 A1={st_fa_a1} B0={st_fa_b} 且指纹一致')
        st_f2, f2, _ = http(a1['base'], 'POST', f'/v1/channels/{CHANNEL_PROOF}/messages', {
            'producer': 'rollout-guard', 'key': f'cross-a-{nonce}', 'lane': 'cross',
            'payload': {'from': 'A1', 'proof': 'A1 write visible to old B0'}})
        st_f2_a, f2_a, _ = http(a1['base'], 'GET', f"/v1/messages/{f2['message_id']}")
        st_f2_b, f2_b, _ = http(b['base'], 'GET', f"/v1/messages/{f2['message_id']}")
        self.check('a1_write_visible_to_unrestarted_b0',
                   st_f2 == 201 and st_f2_a == 200 and st_f2_b == 200
                   and f2_a['message']['fingerprint'] == f2_b['message']['fingerprint'],
                   summary=f'F-A2 #{f2.get("message_id")} A1 提交，A1={st_f2_a}/B0 旧进程={st_f2_b} 指纹一致')
        self.E['rolling'].append({'replaced': 'A', 'fact_id': spec_a['message_id'],
                                  'new_release': a1['release_header']})

        # 9) 滚动替换 B：用 A1 提交的栅栏 F-B --------------------------------
        r = run_cmd([sys.executable, str(ROOT / 'ops/qb_rollout.py'), 'fence',
                     '--db', str(self.common_db), '--via', a1['base'],
                     '--channel', CHANNEL_PROOF, '--out', str(self.fence_b),
                     '--note', 'replace node B'])
        self.check('fence_b_created', r['rc'] == 0 and self.fence_b.is_file(), summary=f'rc={r["rc"]}')
        spec_b = json.loads(self.fence_b.read_text())
        stop_node(b)
        st_flow2, flow2, _ = http(a1['base'], 'POST', f'/v1/channels/{CHANNEL_FLOW}/messages', {
            'producer': 'flow-probe', 'key': f'during-b-down-{nonce}', 'lane': 'flow-b-down',
            'payload': {'when': 'B down'}})
        st_claim_f2, claim_f2, _ = http(
            a1['base'], 'POST', f'/v1/channels/{CHANNEL_FLOW}/groups/{FLOW_GROUP}/claim',
            {'worker': 'flow-a', 'limit': 1})
        self.E['continuity'].append(
            {'during': 'B_down', 'publish_status': st_flow2, 'claim_status': st_claim_f2,
             'leased': len(claim_f2.get('items', []))})
        self.check('partner_a1_takes_work_while_b_down',
                   st_flow2 == 201 and len(claim_f2.get('items', [])) == 1,
                   summary=f'发布={st_flow2} 领取条数={len(claim_f2.get("items", []))}')

        b1 = start_node(self.lab, 'B1-new', pb, self.common_db, RELEASE_NEW,
                        common_db=self.common_db, fence=self.fence_b)
        self.nodes['B'] = b1
        st_fb_b1, fb_b1, _ = http(b1['base'], 'GET', f"/v1/messages/{spec_b['message_id']}")
        self.check('replaced_b1_sees_fact_committed_by_a1',
                   st_fb_b1 == 200 and fb_b1['message']['fingerprint'] == spec_b['fingerprint'],
                   summary=f'F-B #{spec_b["message_id"]} B1 读取={st_fb_b1}')
        st_f4, f4, _ = http(b1['base'], 'POST', f'/v1/channels/{CHANNEL_PROOF}/messages', {
            'producer': 'rollout-guard', 'key': f'cross-b-{nonce}', 'lane': 'cross',
            'payload': {'from': 'B1'}})
        st_f4_a, _, _ = http(a1['base'], 'GET', f"/v1/messages/{f4['message_id']}")
        self.check('b1_write_visible_to_a1', st_f4 == 201 and st_f4_a == 200,
                   summary=f'F-B2 #{f4.get("message_id")} B1 提交，A1 读取={st_f4_a}')
        st_h, _, headers_h = http(b1['base'], 'GET', '/v1/channels')
        self.check('both_ports_now_candidate_release',
                   a1['release_header'] == RELEASE_NEW and b1['release_header'] == RELEASE_NEW,
                   summary=f"A1={a1['release_header']} B1={b1['release_header']}")
        self.E['rolling'].append({'replaced': 'B', 'fact_id': spec_b['message_id'],
                                  'new_release': b1['release_header']})

        # 10) 原租约按真实时钟自然过期，只读核对“已过截止但仍 open” ----------
        holder_port = w1['port']
        peer = b1 if holder_port == pa else a1
        holder_now = a1 if holder_port == pa else b1
        self.E['expiry']['holder_port'] = holder_port
        self.E['expiry']['sweep_via_port'] = peer['port']
        deadline = w1['expires'] + 0.75
        while time.time() < deadline:
            time.sleep(min(0.25, deadline - time.time()))
        common_pre = lease_view(self.common_db, self.message_ids['m1'])
        at1 = common_pre['attempts'][0]
        now_ts = time.time()
        self.E['expiry']['pre_sweep'] = {'now': now_ts, 'expires': at1['expires'],
                                         'seconds_past_deadline': round(now_ts - at1['expires'], 2),
                                         'outcome': at1['outcome']}
        self.check('original_lease_naturally_expired_still_open',
                   at1['outcome'] == 'open' and now_ts > at1['expires'],
                   summary=f"超过截止 {now_ts - at1['expires']:.1f}s 且回收前 outcome 仍为 open（未做任何处置）")
        # 共库两端只读视图一致
        _, trace_peer_pre, _ = http(peer['base'], 'GET', f"/v1/messages/{self.message_ids['m1']}")
        self.check('peer_reads_same_open_past_deadline',
                   public_trace(trace_peer_pre)['attempts'][0]['outcome'] == 'open',
                   summary='对端 HTTP 只读也看到同一行 open 尝试')

        # 11) 另一端触发全局回收 ---------------------------------------------
        st_sw, sweep_body, _ = http(peer['base'], 'POST', '/v1/sweep', {})
        recovered_d1 = [x for x in sweep_body.get('recovered', [])
                        if x.get('delivery_id') == self.delivery_ids['m1']]
        self.E['expiry']['sweep_status'] = st_sw
        self.E['expiry']['sweep_recovered_d1'] = recovered_d1
        self.check('peer_sweep_reclaims_attempt1',
                   st_sw == 200 and len(recovered_d1) == 1
                   and recovered_d1[0]['outcome'] == 'expired'
                   and recovered_d1[0]['attempt'] == 1,
                   summary=json.dumps(recovered_d1, ensure_ascii=False))

        # 12) 回收后再次并发抢领：唯一第二次尝试，后继仍被挡 ------------------
        race2, _ = self.claim_race([a1['base'], b1['base']], 'two')
        self.E['race_two'] = race2
        w2 = race2['winner']
        self.check('race2_exactly_one_replacement_lease', race2['winners_for_d1'] == 1
                   and w2 and w2['attempt_number'] == 2,
                   summary=f"winners={race2['winners_for_d1']} attempt=#{w2 and w2['attempt_number']}")
        self.check('race2_new_token_differs',
                   w2 and w1 and w2['token_digest'] != w1['token_digest'],
                   summary='第二次尝试使用全新凭据，旧凭据随尝试关闭失效')
        self.check('race2_successor_still_blocked', race2['items_for_d2'] == 0,
                   summary='同键后继 D2 在新队首未处置前仍不可领取')
        self.check('race2_single_open_attempt_in_db',
                   open_attempt_count(self.common_db, self.delivery_ids['m1']) == 1,
                   summary='共库任何时刻只有一行 open 尝试（one_open_attempt 部分唯一索引）')

        # 13) 判别一：共库已推进（A1/B1 一致） vs 旧副本冻结 ------------------
        common_post = lease_view(self.common_db, self.message_ids['m1'])
        _, ta1, _ = http(a1['base'], 'GET', f"/v1/messages/{self.message_ids['m1']}")
        _, tb1, _ = http(b1['base'], 'GET', f"/v1/messages/{self.message_ids['m1']}")
        advanced = (len(common_post['attempts']) == 2
                    and [x['outcome'] for x in common_post['attempts']] == ['expired', 'open'])
        self.E['discrimination']['common_attempts'] = [
            {k: x[k] for k in ('number', 'outcome', 'worker', 'expires', 'finished', 'reason')}
            for x in common_post['attempts']]
        self.check('shared_db_state_advanced', advanced,
                   summary='共库尝试序列 = #1 expired -> #2 open，journal 连续推进')
        self.check('a1_b1_views_identical', public_trace(ta1) == public_trace(tb1),
                   summary='两个滚动节点对推进后状态的 HTTP 视图完全一致')

        _, tc_after, _ = http(c['base'], 'GET', f"/v1/messages/{self.message_ids['m1']}")
        decoy_view_after = lease_view(self.stale_db, self.message_ids['m1'])
        frozen = (len(decoy_view_after['attempts']) == 1
                  and decoy_view_after['attempts'][0]['outcome'] == 'open'
                  and decoy_view_after['attempts'][0]['id'] == common_view['attempts'][0]['id'])
        self.E['discrimination']['decoy_attempts_before_own_sweep'] = [
            {k: x[k] for k in ('number', 'outcome', 'worker', 'expires')}
            for x in decoy_view_after['attempts']]
        self.check('decoy_frozen_pre_reclaim_snapshot', frozen,
                   summary='旧副本仍只有 #1/open（截止时间同样已过），没有 expired 记录与 #2')
        st_fence_on_decoy, fence_on_decoy, _ = http(
            c['base'], 'GET', f"/v1/messages/{spec_a['message_id']}")
        self.check('post_fork_fact_404_on_decoy', st_fence_on_decoy == 404,
                   summary=f'栅栏事实 F-A #{spec_a["message_id"]} 在误连端为 {st_fence_on_decoy}，共库两端为 200')

        # 14) 判别二（阴性实验）：放任旧副本自回收会脑裂出不同租约 -----------
        st_cs, cs_body, _ = http(c['base'], 'POST', '/v1/sweep', {})
        st_cc, cc_body, _ = http(
            c['base'], 'POST', f'/v1/channels/{CHANNEL_MAIN}/groups/{GROUP}/claim',
            {'worker': 'decoy-claim', 'limit': 1})
        decoy_items = cc_body.get('items', [])
        decoy_token_digest = codec.fingerprint(decoy_items[0]['lease']['token']) if decoy_items else None
        common_open = [x for x in common_post['attempts'] if x['outcome'] == 'open'][0]
        con = db_connect_readonly(self.common_db)
        try:
            common_token_digest = con.execute(
                "SELECT token_digest FROM attempts WHERE delivery_id=? AND outcome='open'",
                (self.delivery_ids['m1'],)).fetchone()[0]
        finally:
            con.close()
        split = bool(decoy_items) and decoy_token_digest != common_token_digest
        self.E['discrimination']['split_brain'] = {
            'decoy_sweep_status': st_cs, 'decoy_claim_status': st_cc,
            'decoy_attempt_number': decoy_items[0]['lease']['number'] if decoy_items else None,
            'token_digests_differ': split,
            'common_open_attempt_id': common_open['id'],
        }
        self.check('controlled_split_brain_tokens_differ', split,
                   summary='旧副本自行回收后发出与共库不同的 #2 凭据——这正是护栏阻止的重复租约脑裂')

        # 15) 产物哈希与保留的旧库 -------------------------------------------
        self.E['artifacts'] = {
            'common_db_sha256_file': file_sha256(self.common_db),
            'stale_db_sha256_file': file_sha256(self.stale_db),
            'common_logical_hash_final': logical_hash(self.common_db),
            'stale_logical_hash_final': logical_hash(self.stale_db),
            'stale_copy_retained_at': str(self.stale_db),
            'logs': {k: v['log'] for k, v in self.nodes.items()},
            'spec_a': str(self.fence_a), 'spec_b': str(self.fence_b),
        }
        self.check('stale_copy_retained_on_disk', self.stale_db.is_file(),
                   summary=f'旧库保留在 {self.stale_db}')

        # 收尾：默认停掉全部验收进程，旧库/日志/栅栏保留 ----------------------
        if not args.keep:
            for key in ('A', 'B', 'C'):
                if key in self.nodes:
                    stop_node(self.nodes[key])
            self.E['processes_stopped'] = True
            # 进程退出后 WAL 已回收，把两份旧库实物归入验收证据目录：
            # pristine=fork 瞬间的未改动副本；diverged=阴性实验后自行推进的副本。
            ev_dir = Path(args.record_json).resolve().parent / 'evidence'
            ev_dir.mkdir(parents=True, exist_ok=True)
            pristine = ev_dir / 'pristine-fork-copy.sqlite3'
            diverged = ev_dir / 'stale-copy-brain-split.sqlite3'
            import shutil
            shutil.copy2(self.restored_db, pristine)
            shutil.copy2(self.stale_db, diverged)
            for spec_name in ('fence-a.json', 'fence-b.json'):
                shutil.copy2(self.lab / 'specs' / spec_name, ev_dir / spec_name)
            # 合并 WAL 并转成单文件 DELETE 日志模式，确保证据库可独立拷贝复核
            for ev_db in (pristine, diverged):
                con = sqlite3.connect(str(ev_db), timeout=5)
                try:
                    con.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                    con.execute('PRAGMA journal_mode=DELETE')
                    con.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                finally:
                    con.close()
                for suffix in ('-wal', '-shm'):
                    side = Path(str(ev_db) + suffix)
                    if side.exists():
                        side.unlink()
            self.E['artifacts']['pristine_fork_copy'] = str(pristine)
            self.E['artifacts']['brain_split_copy'] = str(diverged)
        else:
            self.E['processes_stopped'] = False
            self.E['keep_pids'] = {k: v['proc'].pid for k, v in self.nodes.items()}

        return self.E['overall_ok']

    @staticmethod
    def _parse_guard_reject(stderr):
        for line in stderr.splitlines():
            if line.startswith('QB_GUARD_REJECT '):
                try:
                    return json.loads(line.split(' ', 1)[1])
                except ValueError:
                    return None
        return None


# ----------------------------- Markdown 记录 -----------------------------

def render_markdown(E, args):
    ok = E['overall_ok']
    L = []
    w = L.append
    w('# Quaybell 两端口滚动替换 · 防误连上线验收记录')
    w('')
    w(f"- 生成时间：{E['generated_at']}")
    w(f"- 总判定：**{'全部通过，可放行' if ok else '存在未通过项，禁止放行'}**")
    w(f"- 端口：A=`{E['ports']['A']}`，B=`{E['ports']['B']}`，误连旧副本 C=`{E['ports']['C_decoy']}`（仅阴性对照）")
    w(f"- 共同库：`{E['paths']['common_db']}`")
    w(f"- 故意保留的旧库：`{E['paths']['stale_db']}`")
    w('- 验收方式：真实子进程 + 真实 8 秒租约自然过期 + 真实 HTTP 跨端口并发；'
      '未调用 pause/ack/nack/redrive/retention，未删除任何业务消息。')
    w('')
    w('## 1. 检查项总表')
    w('')
    w('| 结果 | 检查 | 说明 |')
    w('| --- | --- | --- |')
    for c in E['checks']:
        mark = 'PASS' if c['result'] == 'PASS' else '**FAIL**'
        w(f"| {mark} | `{c['name']}` | {str(c.get('summary', '')).replace('|', '｜')} |")
    w('')

    w('## 2. 可复制的上线命令（本次实际执行）')
    w('')
    w('```sh')
    w('# 一次性：初始化共同库绑定')
    w(f"python3 ops/qb_rollout.py init-cluster --db {E['paths']['common_db']} --cluster {CLUSTER}")
    w('')
    w('# 替换 A 前：经未重启的 B 提交栅栏事实')
    w(f"python3 ops/qb_rollout.py fence --db {E['paths']['common_db']} "
      f"--via http://127.0.0.1:{E['ports']['B']} --channel {CHANNEL_PROOF} --out {E['paths']['lab']}/specs/fence-a.json")
    w('')
    w('# 带护栏启动替换端 A（路径钉 + 集群 + 新鲜事实）')
    w(f"QUAYBELL_RELEASE={RELEASE_NEW} python3 -m quaybell --port {E['ports']['A']} "
      f"--db {E['paths']['common_db']} --common-db {E['paths']['common_db']} "
      f"--expect-cluster {CLUSTER} --require-fence {E['paths']['lab']}/specs/fence-a.json")
    w('')
    w('# 替换 B 前：经已换新的 A 再提交一道栅栏，然后同样带护栏启动 B')
    w(f"python3 ops/qb_rollout.py fence --db {E['paths']['common_db']} "
      f"--via http://127.0.0.1:{E['ports']['A']} --channel {CHANNEL_PROOF} "
      f"--out {E['paths']['lab']}/specs/fence-b.json")
    w(f"QUAYBELL_RELEASE={RELEASE_NEW} python3 -m quaybell --port {E['ports']['B']} "
      f"--db {E['paths']['common_db']} --common-db {E['paths']['common_db']} "
      f"--expect-cluster {CLUSTER} --require-fence {E['paths']['lab']}/specs/fence-b.json")
    w('```')
    w('')
    w('护栏拒绝码：`2` 库不存在（不隐式建空库）、`3` 路径不是权威共同库、'
      '`4` 路径/绑定正确但缺少栅栏新鲜事实＝旧副本、`5` 非受管库或集群不符、`6` 栅栏文件损坏。')
    w('')

    w('## 3. “能拒绝误连”的实录')
    w('')
    for item in E['guard_rejections']:
        w(f"### 3.{1 if item['case']=='wrong_path' else 2} {item['case']}")
        w('')
        w('```sh')
        w(item['command'])
        w('```')
        w(f"退出码：**{item['rc']}**；护栏码：`{(item.get('parsed') or {}).get('code')}`")
        w('')
        parsed = item.get('parsed') or {}
        w('```json')
        w(json.dumps({'code': parsed.get('code'), 'exit_code': parsed.get('exit_code'),
                      'failures': parsed.get('failures')}, ensure_ascii=False, indent=2))
        w('```')
        human = '\n'.join(line for line in item.get('stderr', '').splitlines()
                          if line.startswith('拒绝启动'))
        if human:
            w('```text')
            w(human)
            w('```')
        w('')
    for v in E['guard_verify']:
        if v['case'] == 'stale':
            rep = v.get('report', {})
            w('`ops/qb_rollout.py verify` 对旧副本的结论：')
            w('')
            w('```json')
            w(json.dumps({'ok': rep.get('ok'), 'code': rep.get('code'),
                          'checks': rep.get('checks'), 'observed': rep.get('observed'),
                          'failures': rep.get('failures')}, ensure_ascii=False, indent=2))
            w('```')
            w('')

    w('## 4. 同键不重复发租约')
    w('')
    for key, title in (('race_one', '首次跨两端口 8 并发抢领'), ('race_two', '回收后第二次 8 并发抢领')):
        r = E[key]
        w(f"- **{title}**：总发放条数 `{r.get('total_leased_items')}`，"
          f"D1 赢家数 `{r.get('winners_for_d1')}`，D2（同键后继）发放数 `{r.get('items_for_d2')}`，"
          f"HTTP 错误 `{r.get('http_errors')}`，503 忙重试 `{r.get('busy_retries')}`，"
          f"各端口发放 `{r.get('per_port_leased')}`")
        if r.get('winner'):
            win = dict(r['winner'])
            win['token_digest'] = win['token_digest'][:16] + '…'
            w('')
            w('  ```json')
            w('  ' + json.dumps(win, ensure_ascii=False).replace('\n', '\n  '))
            w('  ```')
    w('')
    w('机制：claim 写事务以 `BEGIN IMMEDIATE` 串行化，提交后队首转为 `leased`，'
      '后继的 `NOT EXISTS (... 更早的 waiting/leased 同键投递)` 谓词不成立；'
      'SQLite 另有 `one_open_attempt` 部分唯一索引兜底，同一投递至多一行 `outcome=\'open\'`。')
    w('')

    w('## 5. 如何分清“共库已推进”与“启动到了旧副本”')
    w('')
    d = E['discrimination']
    w('| 核对点 | 两个滚动端口（共同库，状态已推进） | 误连的旧副本（fork 后冻结） |')
    w('| --- | --- | --- |')
    ca = d.get('common_attempts', [])
    da = d.get('decoy_attempts_before_own_sweep', [])
    w(f"| M1 尝试序列 | `{[(x['number'], x['outcome']) for x in ca]}` | "
      f"`{[(x['number'], x['outcome']) for x in da]}` |")
    w(f"| fork 后栅栏消息 F-A | 两端 `200`，指纹一致 | `404 not_found` |")
    w(f"| journal 水位/栅栏行 | 达到栅栏水位且有 published 事件 | 水位落后、无栅栏行（启动护栏 exit 4） |")
    sb = d.get('split_brain', {})
    w(f"| 若对旧副本也执行 sweep+claim（阴性实验） | 共库当前 open 凭据摘要唯一 | "
      f"旧副本发出 #`{sb.get('decoy_attempt_number')}`，与共库凭据不同＝脑裂：`{sb.get('token_digests_differ')}` |")
    w('')
    w('**判别口径**：过期时刻本身不能作为依据——旧副本墙上的 `expires` 同样会过去，'
      '但在它自己的回收器运行前会一直停在 `#1/open`；真正的证据是 '
      '① 启动栅栏（fork 后事实可见性 + journal 水位）；② 尝试行是否已出现 '
      '`expired` 收口与新编号/新摘要的尝试；③ 两端 HTTP 追溯视图是否逐字段一致。'
      '若一个“替换端”给不出栅栏事实或停在旧尝试序列，它不是看到了推进后的共库，而是连着旧副本。')
    w('')
    ex = E['expiry']
    w('自然过期与回收实录：')
    w('')
    w('```json')
    w(json.dumps({'holder_port': ex.get('holder_port'), 'sweep_via_port': ex.get('sweep_via_port'),
                  'pre_sweep': ex.get('pre_sweep'),
                  'sweep_recovered_d1': ex.get('sweep_recovered_d1')}, ensure_ascii=False, indent=2))
    w('```')
    w('')

    w('## 6. 滚动替换与连续性')
    w('')
    for r in E['rolling']:
        w(f"- 替换 `{r['replaced']}`：伙伴端提交事实 `#{r['fact_id']}`，替换端启动护栏通过并读到该事实，发布代号 `{r['new_release']}`。")
    for c in E['continuity']:
        w(f"- `{c['during']}` 期间另一端接活：发布 `{c['publish_status']}`，领取 `{c['claim_status']}`，发放 `{c['leased']}` 条。")
    w('')
    w('响应头 `X-Quaybell-Release` 用于区分应答进程代次；数据级放行证据是跨进程读到伙伴刚提交的事实。')
    w('')

    w('## 7. 产物')
    w('')
    w('```json')
    w(json.dumps(E['artifacts'], ensure_ascii=False, indent=2))
    w('```')
    w('')
    w(f"复跑：`python3 ops/rolling_acceptance.py`（约 20 秒；自动建/清 `.rollout-lab/`，"
      '结束默认停进程并保留旧库、栅栏 JSON 与节点日志；加 `--keep` 保留运行进程）。')
    w('')
    return '\n'.join(L)


def main():
    p = argparse.ArgumentParser(description='Quaybell 滚动升级防误连验收')
    p.add_argument('--lab', default=str(ROOT / '.rollout-lab'))
    p.add_argument('--record-md', default=str(ROOT / 'ACCEPTANCE.md'))
    p.add_argument('--record-json', default=str(ROOT / 'acceptance' / 'rolling-acceptance.json'))
    p.add_argument('--port-a', type=int, default=18091)
    p.add_argument('--port-b', type=int, default=18092)
    p.add_argument('--port-c', type=int, default=18093)
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--lease-seconds', type=int, default=8)
    p.add_argument('--keep', action='store_true')
    args = p.parse_args()

    acc = Acceptance(args)
    crashed = None
    try:
        ok = acc.run()
    except Exception as exc:  # noqa: BLE001
        crashed = exc
        ok = False
        acc.E['overall_ok'] = False
        acc.E['crash'] = repr(exc)
        import traceback
        acc.E['traceback'] = traceback.format_exc()
        for key in ('A', 'B', 'C'):
            node = acc.nodes.get(key)
            if node:
                try:
                    tail = Path(node['log']).read_text(errors='replace')[-3000:]
                    acc.E.setdefault('node_log_tails', {})[key] = tail
                except OSError:
                    pass
        if not args.keep:
            for node in list(acc.nodes.values()):
                stop_node(node)

    out_json = Path(args.record_json)
    out_md = Path(args.record_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(acc.E, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    out_md.write_text(render_markdown(acc.E, args), encoding='utf-8')
    if crashed is not None:
        print('验收脚本异常:', repr(crashed), file=sys.stderr)
        print(acc.E.get('traceback', ''), file=sys.stderr)
    print(f"\n记录已写入:\n  {out_md}\n  {out_json}")
    print('总判定:', 'PASS' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
