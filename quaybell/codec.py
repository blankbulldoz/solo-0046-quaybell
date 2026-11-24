import hashlib
import json

from .errors import invalid


def pack(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(',', ':'))


def unpack(raw):
    return json.loads(raw)


def fingerprint(raw):
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def object_body(raw):
    try:
        value = unpack(raw)
        pack(value)
    except (UnicodeError, ValueError, RecursionError):
        raise invalid('请求必须是有限且有效的 JSON') from None
    if not isinstance(value, dict):
        raise invalid('请求内容必须是 JSON 对象')
    return value
