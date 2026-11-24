import argparse
import logging
from ipaddress import ip_address
from pathlib import Path

from .server import RelayServer
from .persistence import RelayPersistence


def main():
    parser = argparse.ArgumentParser(description='启动 Quaybell 持久消息中继')
    parser.add_argument('--host', default='127.0.0.1', choices=['127.0.0.1'])
    parser.add_argument('--port', default=8086, type=int)
    parser.add_argument('--db', default=Path('.quaybell/quaybell.sqlite3'), type=Path)
    parser.add_argument('--demo', action='store_true', help='装载示范频道（如已有同名频道则保留它）')
    args = parser.parse_args()
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
