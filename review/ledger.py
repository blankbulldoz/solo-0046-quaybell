"""带引用的可更新账本。

职责边界：
- ``verify`` 只核对账本里的每条引用能否在材料里逐字段对上，以及证据摘要是否自洽。
- ``import-receipt`` 导入只含父子分发号的迟来回执。它只允许把旧链的「有直接后继」
  抬到双源交叉，绝不触碰新消息那一行的处置结论。
- ``accept-supplement`` 验收时间接近但链路不同的补充材料，给出关联成立到哪一层，
  以及为什么仍需人核对。
- ``show`` 把账本渲染成带引用的 Markdown。

账本是复核工具，不是中继运行时的一部分；它从不写入 Quaybell 存储。
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from quaybell.codec import fingerprint, pack

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / 'review' / 'ledger.json'
EVIDENCE = ROOT / 'review' / 'evidence'
RENDERED = ROOT / 'review' / 'LEDGER.md'

ADOPTED = ('采用', '采用（收窄）', '采用（限旧链）')
LADDER = {step['id']: step for step in json.loads(LEDGER.read_text(encoding='utf-8'))['step_ladder']}


class Refusal(Exception):
    """账本拒绝一次更新，因为会写出材料不支持的结论。"""


def load(path=LEDGER):
    ledger = json.loads(Path(path).read_text(encoding='utf-8'))
    ledger.setdefault('sample', {}).setdefault('run', 'run-001')
    return ledger


def save(ledger, path=LEDGER):
    checks = invariants(ledger)
    if not checks['ok']:
        raise Refusal('；'.join(checks['problems']))
    Path(path).write_text(json.dumps(ledger, ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
    return ledger


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def resolve(document, dotted):
    node = document
    for part in dotted.split('.'):
        if isinstance(node, list):
            node = node[int(part)]
        elif isinstance(node, dict):
            node = node[part]
        else:
            raise KeyError(dotted)
    return node


def new_trace_path(ledger):
    run = ledger.get('sample', {}).get('run', 'run-001')
    return EVIDENCE / run / 'evidence-new.json'


def new_message_terminal(path):
    """新消息唯一保留的证据快照里，尝试的终态。"""
    trace = read_json(path)['trace']
    delivery = next(item for item in trace['deliveries']
                    if item['message_id'] == trace['message']['id'])
    attempts = delivery['attempts']
    return dict(state=delivery['state'],
                outcome=attempts[-1]['outcome'] if attempts else None,
                finished=attempts[-1]['finished'] if attempts else None)


def check_manifest(run_dir):
    manifest = read_json(Path(run_dir) / 'MANIFEST.json')
    problems = []
    for name, entry in manifest['files'].items():
        path = Path(run_dir) / name
        if not path.exists():
            problems.append(f'{path} 缺失')
            continue
        if entry['digest_of'] == 'record':
            record = read_json(path)
            claimed = record.pop('sha256')
            actual = fingerprint(pack(record))
            if claimed != entry['sha256'] or actual != claimed:
                problems.append(f'{name} 摘要不自洽')
        else:
            actual = fingerprint(path.read_text(encoding='utf-8'))
            if actual != entry['sha256']:
                problems.append(f'{name} 文件摘要不符')
    return problems


def citation_problems(ledger):
    problems = []
    digest_cache = {}
    for claim in ledger['claims']:
        for cite in claim['citations']:
            path = ROOT / cite['file']
            if not path.exists():
                problems.append(f"{claim['id']}: {cite['file']} 不存在")
                continue
            if path.suffix == '.json' and path.name.startswith('evidence-'):
                if path.name not in digest_cache:
                    record = read_json(path)
                    claimed = record.pop('sha256')
                    digest_cache[path.name] = fingerprint(pack(record)) == claimed
                if not digest_cache[path.name]:
                    problems.append(f"{claim['id']}: {path.name} 记录摘要不自洽")
            try:
                actual = resolve(read_json(path), cite['path'])
            except (KeyError, IndexError, ValueError, TypeError):
                problems.append(f"{claim['id']}: {cite['file']}#{cite['path']} 无法解析")
                continue
            if actual != cite['expect']:
                problems.append(
                    f"{claim['id']}: {cite['file']}#{cite['path']} 期望 {cite['expect']!r}，实为 {actual!r}")
    return problems


def invariants(ledger):
    problems = list(citation_problems(ledger))
    trace_path = new_trace_path(ledger)
    terminal = new_message_terminal(trace_path) if trace_path.exists() else None
    if terminal is None:
        problems.append(f'缺少新消息证据 {trace_path}')
    seen = set()
    for claim in ledger['claims']:
        if claim['id'] in seen:
            problems.append(f"{claim['id']} 重复")
        seen.add(claim['id'])
        if claim['step'] not in LADDER:
            problems.append(f"{claim['id']}: 未知证据台阶 {claim['step']}")
        if claim['asserts_handled_of_new_message'] and terminal is not None:
            # 无论采用还是否定，都拿保留证据的终态做硬校验，避免「手记一改就过关」。
            if terminal['outcome'] != 'acked' and claim['disposition'] in ADOPTED:
                problems.append(
                    f"{claim['id']}: 采用「新消息已处理」，但保留证据里尝试终态是 "
                    f"{terminal['outcome']!r}，不是 acked")
            if terminal['outcome'] == 'acked' and claim['disposition'] not in ADOPTED:
                problems.append(
                    f"{claim['id']}: 保留证据里新消息已确认，账本却未采用")
        if claim['subject'] == 'new-message' and claim['asserts_handled_of_new_message'] and not claim.get('open'):
            problems.append(f"{claim['id']}: 否定「已处理」却不留待核项，交接会丢掉线索")
    materials = {item['id']: item for item in ledger['materials']}
    for claim in ledger['claims']:
        for ref in claim.get('material_refs', []):
            if ref not in materials:
                problems.append(f"{claim['id']}: 引用了未登记的锚点 {ref}")
            elif claim['subject'] == 'new-message' and materials[ref]['kind'] == 'redrive-receipt':
                problems.append(
                    f"{claim['id']}: 用重投回执支撑新消息的处置；回执只能证明旧链有后继")
    return {'ok': not problems, 'problems': problems}


def now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def append_event(ledger, kind, summary):
    ledger['revision'] += 1
    ledger['updated_at'] = now()
    ledger['events'].append({'revision': ledger['revision'], 'at': ledger['updated_at'],
                             'kind': kind, 'summary': summary})


def claim(ledger, key):
    for item in ledger['claims']:
        if item['id'] == key:
            return item
    raise KeyError(key)


def receipt_import(ledger, path):
    receipt = read_json(path)
    if receipt.get('format') != 'quaybell-redrive-receipt/1':
        raise Refusal('不是重投回执格式')
    parent, child = receipt.get('parent_delivery'), receipt.get('child_delivery')
    if not (isinstance(parent, int) and isinstance(child, int)):
        raise Refusal('回执必须给出整数父子分发号')
    facts = read_json(EVIDENCE / 'run-001' / 'facts.json')
    redrive = facts['original_redrive']
    if redrive['source_id'] != parent or redrive['delivery_id'] != child:
        raise Refusal(f'回执 {parent}→{child} 与本机样本旧链 {redrive["source_id"]}→'
                      f'{redrive["delivery_id"]} 对不上，不能认领')
    trail = read_json(EVIDENCE / 'run-001' / 'evidence-original.json')
    deliveries = {item['id']: item for item in trail['trace']['deliveries']}
    if deliveries[child]['parent_id'] != parent:
        raise Refusal('保留证据里父子关系与回执不一致')
    if deliveries[child]['state'] != 'acked':
        raise Refusal('保留证据里子分发并未收尾，回执不足以说明后继已终结')

    item = claim(ledger, 'C4')
    if 'receipt-1-2' in item.get('material_refs', []):
        raise Refusal('该回执已导入过')
    ledger['materials'].append({
        'id': 'receipt-1-2',
        'kind': 'redrive-receipt',
        'file': str(Path(path).resolve().relative_to(ROOT)),
        'scope': '只含父子分发号；无频道、时间、处置结果或凭据。',
        'proves': '旧链 1 → 2 存在直接后继，且 2 已确认。',
        'does_not_prove': '不涉及新消息 #2 / 分发 3 的任何处置。'
    })
    item.update({
        'assertion': ('回执中的父子分发号与本机保留材料对上：旧消息 #1 的分发 1 被明确拒绝后，'
                      '重投追加直接后继分发 2，分发 2 已在 05:00:06 确认。'),
        'step': 'S3',
        'verdict': '一致（仅旧链）',
        'disposition': '采用（限旧链）',
        'qualifier': ('回执与保留证据两条独立材料在 1→2 与其后继已确认上一致；'
                      '这只证明旧链有后继，不能把新消息那一行改成已处理。'),
        'material_refs': ['receipt-1-2'],
        'citations': [
            {'file': 'review/evidence/run-001/facts.json', 'path': 'original_redrive.source_id', 'expect': 1},
            {'file': 'review/evidence/run-001/facts.json', 'path': 'original_redrive.delivery_id', 'expect': 2},
            {'file': 'review/evidence/run-001/evidence-original.json', 'path': 'trace.deliveries.1.parent_id', 'expect': 1},
            {'file': 'review/evidence/run-001/evidence-original.json', 'path': 'trace.deliveries.1.state', 'expect': 'acked'},
            {'file': 'review/evidence/run-001/evidence-original.json', 'path': 'trace.message.id', 'expect': 1},
            {'file': 'review/supplements/receipt-1-2.json', 'path': 'parent_delivery', 'expect': 1},
            {'file': 'review/supplements/receipt-1-2.json', 'path': 'child_delivery', 'expect': 2}
        ],
        'open': []
    })
    append_event(ledger, 'receipt-imported',
                 '导入迟来重投回执 1→2：仅抬升旧链为双源交叉，新消息 #2 / 分发 3 的处置不变（仍为未处理）。')
    return ledger


def supplement_accept(ledger, path):
    document = read_json(path)
    if document.get('format') != 'quaybell-supplement/1':
        raise Refusal('不是补充材料格式')
    problems = []
    for cite in document.get('citations', []):
        actual = resolve(read_json(ROOT / cite['file']), cite['path'])
        if actual != cite['expect']:
            problems.append(f"{cite['file']}#{cite['path']} 期望 {cite['expect']!r}，实为 {actual!r}")
    if problems:
        raise Refusal('补充材料无法在保留证据中对上：' + '；'.join(problems))

    identity = document['identity']
    new_trace = read_json(EVIDENCE / 'run-001' / 'evidence-new.json')
    new_message = new_trace['trace']['message']
    # 证据里的信封把标签和载荷收在 envelope 下，生产键字段名为 dedup_key。
    pairs = {
        'producer': new_message.get('producer'),
        'key': new_message.get('dedup_key'),
        'lane': new_message.get('lane'),
        'tags': new_message.get('envelope', {}).get('tags'),
        'payload': new_message.get('envelope', {}).get('payload'),
        'fingerprint': new_message.get('fingerprint'),
    }
    same_fields = [name for name, value in pairs.items() if identity.get(name) == value]
    differing = [name for name in pairs if name not in same_fields]
    if identity['fingerprint'] != new_message['fingerprint']:
        raise Refusal('补充材料的信封指纹与本告警不同，不能讨论关联')
    if identity['channel'] == new_message['channel']:
        raise Refusal('补充材料与本告警同频道，这是同一链路而不是补充链路')

    item = claim(ledger, 'C6')
    if 'supplement-mirror' in item.get('material_refs', []):
        raise Refusal('该补充材料已验收过')
    ledger['materials'].append({
        'id': 'supplement-mirror',
        'kind': 'peer-trace',
        'file': str(Path(path).resolve().relative_to(ROOT)),
        'scope': '另一频道 alert-mirror 上的同信封消息 #3 / 分发 4。',
        'proves': '字段级同构：生产者、生产键、顺序键、标签、载荷、指纹全部一致，时间相差 5 秒。',
        'does_not_prove': '不证明它与告警本体是同一次分发或同一次业务事件；频道不同、消息号不同、父分发为空。'
    })
    item.update({
        'assertion': ('本机可核对：补充材料自带 identity 指向 alert-mirror 的消息 #3 / 分发 4，'
                      '该分发在 05:21:00 被确认；它与告警本体 alert-desk 的消息 #2 / 分发 3 '
                      '信封逐字段相同，但链路身份不同。'),
        'step': 'S2',
        'verdict': '字段一致，链路未合并',
        'disposition': '仅登记，不并账',
        'qualifier': (
            f"共 {len(same_fields)} 个身份字段（{'、'.join(same_fields)}）一致"
            + (f"，另有 {'、'.join(differing)} 不一致" if differing else '')
            + f"；但 channel={identity['channel']}、message_id={identity['message_id']}、"
            f"delivery_id={identity['delivery_id']}、parent_id=null，且发布于 05:12:05"
            '（新消息发布后 5 秒）。字段同构可以解释成镜像转发，也可能只是另一处按同一模板重发；'
            '证据本身分不开这两种解释。'
        ),
        'associations': [
            {'layer': '字段级', 'status': '成立', 'basis': '信封与指纹逐字段相同'},
            {'layer': '链路级', 'status': '未成立', 'basis': '频道与消息身份不同，分发 4 无父节点'},
            {'layer': '业务级', 'status': '需人核对', 'basis': '缺少镜像转发规则或生产侧同批次说明'}
        ],
        'material_refs': ['supplement-mirror'],
        'citations': document['citations'] + [
            {'file': 'review/evidence/run-001/evidence-new.json', 'path': 'trace.message.channel', 'expect': 'alert-desk'},
            {'file': 'review/evidence/run-001/evidence-new.json', 'path': 'trace.message.id', 'expect': 2},
            {'file': 'review/evidence/run-001/facts.json', 'path': 'new_publish.message_id', 'expect': 2},
            {'file': 'review/evidence/run-001/facts.json', 'path': 'mirror_publish.message_id', 'expect': 3}
        ],
        'open': ['要把两条链路定为人核对后的关联，需要生产侧提供 alert-mirror 的转发规则，'
                 '人核对后再决定是否在同一次告警下并账；账本不代替这一步。']
    })
    append_event(ledger, 'supplement-accepted',
                 '验收补充材料：字段级同构成立，链路级不成立，业务级留给人核对；未并入告警本体。')
    return ledger


def describe(ledger):
    lines = [f"# {ledger['title']}", '',
             f"账本修订 r{ledger['revision']}，更新于 {ledger['updated_at']}。", '',
             f"> {ledger['scope']}", '',
             '## 证据台阶', '',
             '| 台阶 | 含义 |', '| --- | --- |']
    for step in ledger['step_ladder']:
        lines.append(f"| {step['id']} {step['name']} | {step['meaning']} |")
    lines += ['', '## 声明', '',
              '| 声明 | 内容 | 能确认到 | 判定 | 处置 | 会否改写新消息那一行 |',
              '| --- | --- | --- | --- | --- | --- |']
    for item in ledger['claims']:
        affects = '会' if item['asserts_handled_of_new_message'] else '不会'
        lines.append(f"| {item['id']} {item['note']} | {item['subject']} | "
                     f"{item['step']} {LADDER[item['step']]['name']} | {item['verdict']} | "
                     f"{item['disposition']} | {affects} |")
    for item in ledger['claims']:
        lines += ['', f"### {item['id']} · {item['note']}", '',
                  item['assertion'], '', f"- 台阶：{item['step']} {LADDER[item['step']]['name']}",
                  f"- 判定：{item['verdict']}；处置：{item['disposition']}",
                  f"- 限定：{item['qualifier']}"]
        if item.get('associations'):
            lines += ['', '| 关联层 | 是否成立 | 依据 |', '| --- | --- | --- |']
            for row in item['associations']:
                lines.append(f"| {row['layer']} | {row['status']} | {row['basis']} |")
        if item['citations']:
            lines += ['', '| 引用 | 字段 | 期望值 |', '| --- | --- | --- |']
            for cite in item['citations']:
                lines.append(f"| `{cite['file']}` | `{cite['path']}` | `{json.dumps(cite['expect'], ensure_ascii=False)}` |")
        if item['open']:
            lines += ['', '待核：'] + [f"- {entry}" for entry in item['open']]
    lines += ['', '## 材料锚点', '', '| 锚点 | 类型 | 文件 | 能证明 | 不能证明 |', '| --- | --- | --- | --- | --- |']
    for material in ledger['materials']:
        lines.append(f"| {material['id']} | {material['kind']} | `{material['file']}` | "
                     f"{material['proves']} | {material['does_not_prove']} |")
    if not ledger['materials']:
        lines.append('| — | — | — | 尚未导入外部材料 | — |')
    lines += ['', '## 修订记录', '']
    for event in ledger['events']:
        lines.append(f"- r{event['revision']} · {event['at']} · {event['kind']}：{event['summary']}")
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description='核对、更新并渲染复盘账本')
    parser.add_argument('--ledger', type=Path, default=LEDGER)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('verify', help='核对全部引用、摘要与不变量')
    sub.add_parser('show', help='渲染 Markdown 账本')
    receipt = sub.add_parser('import-receipt', help='导入迟来的重投回执')
    receipt.add_argument('path', type=Path)
    supplement = sub.add_parser('accept-supplement', help='验收时间接近但链路不同的补充材料')
    supplement.add_argument('path', type=Path)
    args = parser.parse_args(argv)

    if args.command == 'verify':
        ledger = load(args.ledger)
        problems = check_manifest(new_trace_path(ledger).parent) + invariants(ledger)['problems']
        for problem in problems:
            print(f'FAIL {problem}')
        print(f"核对完成：{len(ledger['claims'])} 条声明，{len(problems)} 个问题")
        return 1 if problems else 0

    if args.command == 'show':
        ledger = load(args.ledger)
        text = describe(ledger)
        RENDERED.write_text(text, encoding='utf-8')
        print(text)
        print(f'已写入 {RENDERED.relative_to(ROOT)}')
        return 0

    if args.command == 'import-receipt':
        ledger = load(args.ledger)
        try:
            receipt_import(ledger, args.path)
            save(ledger, args.ledger)
        except Refusal as exc:
            print(f'拒绝：{exc}')
            return 2
        print('已导入重投回执；旧链 C4 抬到 S3，新消息行未改动。')
        return 0

    if args.command == 'accept-supplement':
        ledger = load(args.ledger)
        try:
            supplement_accept(ledger, args.path)
            save(ledger, args.ledger)
        except Refusal as exc:
            print(f'拒绝：{exc}')
            return 2
        print('已验收补充材料：字段级成立、链路级不成立、业务级留给人核对。')
        return 0

    raise AssertionError(args.command)


if __name__ == '__main__':
    sys.exit(main())
