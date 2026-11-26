import logging
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import time
from urllib.parse import parse_qs, urlsplit

from . import api_reads, api_writes, presentation, protocol, routes
from .errors import RelayError, missing

LOG = logging.getLogger(__name__)


class RelayServer(ThreadingHTTPServer):
    def __init__(self, address, persistence, clock=time):
        self.persistence, self.clock = persistence, clock
        super().__init__(address, RelayRequest)

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(8)
        return sock, address


class RelayRequest(BaseHTTPRequestHandler):
    server_version = 'Quaybell/1'
    sys_version = ''

    def do_GET(self):
        self.handle_request('GET')

    def do_POST(self):
        self.handle_request('POST')

    def handle_request(self, method):
        try:
            reply = self.resolve_route(method)
        except RelayError as exc:
            reply = protocol.Reply({'error': exc.code, 'message': str(exc)}, exc.status)
        except sqlite3.OperationalError as exc:
            if getattr(exc, 'sqlite_errorcode', 0) & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                reply = protocol.Reply({'error': 'busy', 'message': '中继存储繁忙，请稍后按原身份重试'}, 503)
            else:
                LOG.exception('存储操作失败')
                reply = protocol.Reply({'error': 'internal', 'message': '存储操作失败'}, 500)
        except TimeoutError:
            reply = protocol.Reply({'error': 'timeout', 'message': '读取请求超时'}, 408)
        body = reply.body()
        try:
            self.send_response(reply.status)
            self.send_header('Content-Type', reply.content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-persistence')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; frame-ancestors 'none'")
            if reply.filename:
                self.send_header('Content-Disposition', f'attachment; filename="{reply.filename}"')
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return

    def resolve_route(self, method):
        parsed = urlsplit(self.path)
        path = parsed.path
        route = routes.match(method, path)
        if route is None:
            if method == 'GET' and not path.startswith('/v1/'):
                return presentation.serve(path)
            raise missing('找不到此接口')
        action, args = route
        if method == 'GET':
            query = parse_qs(parsed.query)
            with self.server.persistence.session() as db:
                return api_reads.read(db, action, args, self.server.clock(), query)
        raw = protocol.receive(self)
        read_only = action in ('preview', 'audit_query')
        with self.server.persistence.session(write=not read_only) as db:
            if action == 'preview':
                return api_reads.check_route(db, args[0], raw)
            if action == 'audit_query':
                return api_reads.audit_route(db, args[0], raw, self.server.clock())
            return api_writes.write(db, action, args, raw, self.server.clock())
