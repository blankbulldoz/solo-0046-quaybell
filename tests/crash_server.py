"""Subprocess entry point for repeatable publish-crash acceptance tests.

The crash switches are intentionally test-only and are installed by monkeypatching
the normal application modules.  They let a test stop a real server process at two
different boundaries:

* ``fanout_after_first``: after the first matching delivery INSERT returns, while
  the publication transaction is still open and before later groups are routed.
* ``after_commit_before_response``: after the write transaction has committed, but
  before ``Reply.body()`` is serialized and therefore before any HTTP bytes are sent.
"""
import argparse
import json
import os
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quaybell.persistence import RelayPersistence  # noqa: E402
from quaybell.server import RelayServer  # noqa: E402


def write_marker(payload):
    marker_path = os.environ['QUAYBELL_CRASH_MARKER']
    payload = dict(payload)
    payload['pid'] = os.getpid()
    temporary = marker_path + '.tmp'
    with open(temporary, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, marker_path)


def install_fault_hooks(boundary):
    if boundary == 'fanout_after_first':
        from quaybell import delivery

        original_add = delivery.add
        state = {'calls': 0}

        def add_and_kill_after_first(db, message_id, group_id, lane, version, ready_at, parent_id=None):
            delivery_id = original_add(
                db, message_id, group_id, lane, version, ready_at, parent_id
            )
            state['calls'] += 1
            if state['calls'] == 1:
                write_marker({
                    'boundary': boundary,
                    'phase': 'after_first_matching_delivery_insert_before_second',
                    'message_id': message_id,
                    'subscription_id': group_id,
                    'delivery_id': delivery_id,
                    'committed': False,
                })
                os.kill(os.getpid(), signal.SIGKILL)
            return delivery_id

        delivery.add = add_and_kill_after_first

    elif boundary == 'after_commit_before_response':
        from quaybell.protocol import Reply

        original_body = Reply.body

        def body_and_kill_after_commit(self):
            data = getattr(self, 'data', None)
            if (isinstance(data, dict)
                    and 'fanout' in data
                    and data.get('repeated') is False):
                write_marker({
                    'boundary': boundary,
                    'phase': 'transaction_committed_before_response_body',
                    'reply': data,
                    'committed': True,
                })
                os.kill(os.getpid(), signal.SIGKILL)
            return original_body(self)

        Reply.body = body_and_kill_after_commit

    elif boundary in ('', 'off', 'normal'):
        return
    else:
        raise RuntimeError(f'unknown crash boundary: {boundary}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True, type=Path)
    parser.add_argument('--ready-file', required=True, type=Path)
    parser.add_argument('--port', default=0, type=int)
    args = parser.parse_args()

    boundary = os.environ.get('QUAYBELL_CRASH_BOUNDARY', '')
    install_fault_hooks(boundary)

    persistence = RelayPersistence(args.db)
    with RelayServer(('127.0.0.1', args.port), persistence) as server:
        ready = {'port': server.server_port, 'pid': os.getpid(), 'boundary': boundary or 'normal'}
        args.ready_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.ready_file.with_suffix(args.ready_file.suffix + '.tmp')
        temporary.write_text(json.dumps(ready), encoding='utf-8')
        os.replace(temporary, args.ready_file)
        server.serve_forever(poll_interval=0.02)


if __name__ == '__main__':
    main()
