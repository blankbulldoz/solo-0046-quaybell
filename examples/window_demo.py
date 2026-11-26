"""生成两次交班窗口件，以及接收端会识破的两类改动样本。"""
import argparse
import copy
import json
from pathlib import Path

from quaybell.codec import fingerprint, pack

from .session import session
from .walkthrough import window_scenario


def _write_json(path, document):
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description='生成 Quaybell 时间窗取证演示文件')
    parser.add_argument('--out', type=Path, default=Path('/tmp/quaybell-window-demo'),
                        help='演示 JSON 的输出目录')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    with session() as client:
        scenario = window_scenario(client)

    fact_resealed = copy.deepcopy(scenario['tampered_fact'])
    fact_resealed['sha256'] = fingerprint(pack(
        {key: value for key, value in fact_resealed.items() if key != 'sha256'}))

    files = {
        'handoff-1-claim-and-deadline.json': scenario['first'],
        'handoff-2-ack-and-reclaim.json': scenario['second'],
        'tampered-in-window-fact.json': scenario['tampered_fact'],
        'tampered-in-window-fact-resealed.json': fact_resealed,
        'mislabeled-outside-event-old-digest.json': scenario['mislabeled'],
        'mislabeled-outside-event-resealed.json': scenario['resealed'],
    }
    for name, document in files.items():
        _write_json(args.out / name, document)

    print(json.dumps({
        'out': str(args.out),
        'message_id': scenario['message_id'],
        'original_window_one_sha256': scenario['first']['sha256'],
        'files': sorted(files),
        'verify': 'python3 -I quaybell/verify.py <file.json>',
        'pinned_verify': 'python3 -I quaybell/verify.py tampered-in-window-fact-resealed.json '
                        f"--expect-sha256 {scenario['first']['sha256']}",
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
