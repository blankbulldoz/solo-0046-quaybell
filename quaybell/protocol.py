from dataclasses import dataclass

from . import codec
from .errors import RelayError

JSON = 'application/json; charset=utf-8'
MAX_REQUEST = 32768
VERIFY_MAX_REQUEST = 262144


@dataclass(frozen=True)
class Reply:
    data: object
    status: int = 200
    content_type: str = JSON
    filename: str | None = None

    def body(self):
        if isinstance(self.data, bytes):
            return self.data
        return codec.pack(self.data).encode('ascii')


def receive(request, max_bytes=MAX_REQUEST):
    if request.headers.get('Transfer-Encoding'):
        raise RelayError(400, 'invalid_framing', '请求须声明固定长度')
    if request.headers.get_content_type() != 'application/json':
        raise RelayError(415, 'unsupported_media_type', '请求须使用 application/json')
    lengths = request.headers.get_all('Content-Length', [])
    if len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdecimal():
        raise RelayError(400, 'invalid_length', '请求须声明有效且唯一的长度')
    if len(lengths[0]) > 8 or int(lengths[0]) > max_bytes:
        raise RelayError(413, 'request_too_large', f'请求最多 {max_bytes // 1024} KiB')
    length = int(lengths[0])
    raw = request.rfile.read(length)
    if len(raw) != length:
        raise RelayError(400, 'short_request', '请求体不完整')
    return codec.object_body(raw)
