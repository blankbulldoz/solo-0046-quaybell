"""外部接收人离线复核 Quaybell 证据包：python3 -I quaybell/verify.py 文件.json。"""
import argparse
import json
import sys
from pathlib import Path

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from quaybell.evidence import verify_document
else:
    from .evidence import verify_document


def main(argv=None):
    parser = argparse.ArgumentParser(description='复核 Quaybell 窗口证据的摘要与时间口径')
    parser.add_argument('path', type=Path, help='接收到的 JSON 证据文件')
    parser.add_argument('--expect-source-commitment',
                        help='通过独立渠道获得的原整包证据摘要（可选，含或不含 sha256: 前缀）')
    parser.add_argument('--expect-sha256',
                        help='通过独立渠道钉住的窗口件规范化摘要（可选，含或不含 sha256: 前缀）')
    args = parser.parse_args(argv)

    try:
        document = json.loads(args.path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        result = {'ok': False, 'errors': [{'code': 'document_unreadable', 'message': str(exc)}]}
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 2

    result = verify_document(document, args.expect_source_commitment, args.expect_sha256)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get('ok') else 1


if __name__ == '__main__':
    sys.exit(main())
