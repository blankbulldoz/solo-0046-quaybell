import argparse
import logging
import os
from ipaddress import ip_address
from pathlib import Path

from .rollout import enforce_for_cli
from .server import RelayServer
from .persistence import RelayPersistence


def main():
    parser = argparse.ArgumentParser(description='启动 Quaybell 持久消息中继')
    parser.add_argument('--host', default='127.0.0.1', choices=['127.0.0.1'])
    parser.add_argument('--port', default=8086, type=int)
    parser.add_argument('--db', default=Path('.quaybell/quaybell.sqlite3'), type=Path)
    parser.add_argument('--demo', action='store_true', help='装载示范频道（如已有同名频道则保留它）')
    parser.add_argument('--release', default=os.environ.get('QUAYBELL_RELEASE', 'unversioned'),
                        help='本次进程的发布代号，会通过 X-Quaybell-Release 响应头暴露')
    parser.add_argument('--common-db', type=Path, default=None,
                        help='滚动上线的权威共同库路径；与 --db 的 realpath 不一致即拒绝启动')
    parser.add_argument('--expect-cluster', default=None,
                        help='要求候选库 rollout_binding 中的集群标识与此一致')
    parser.add_argument('--require-fence', type=Path, default=None,
                        help='栅栏 JSON：候选库必须能看到未重启端刚提交的事实，否则按旧副本拒绝')
    args = parser.parse_args()
    os.environ['QUAYBELL_RELEASE'] = args.release

    if args.require_fence is not None or args.expect_cluster is not None or args.common_db is not None:
        enforce_for_cli(db_path=args.db, common_db=args.common_db,
                        expected_cluster=args.expect_cluster, fence_path=args.require_fence)

    logging.basicConfig(level=logging.INFO)
    persistence = RelayPersistence(args.db)
    if args.demo:
        from .seed import populate
        populate(persistence)
    with RelayServer((str(ip_address(args.host)), args.port), persistence) as server:
        print(f'Quaybell 操作台 http://{args.host}:{server.server_port}', flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
