import re

from .errors import invalid

NAME = re.compile(r'[a-z][a-z0-9-]{0,47}\Z')
TOKEN = re.compile(r'[A-Za-z0-9_./:-]{1,96}\Z')


def name(value, label):
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise invalid(f'{label}须以小写字母起头，最多 48 个小写字母、数字或短横线')
    return value


def token(value, label):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        raise invalid(f'{label}格式无效')
    return value


def integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise invalid(f'{label}须为 {low}～{high} 的整数')
    return value


def boolean(value, label):
    if type(value) is not bool:
        raise invalid(f'{label}须为布尔值')
    return value


def require(data, keys, optional=()):
    if not set(keys) <= data.keys() or data.keys() - set(keys) - set(optional):
        raise invalid('请求字段缺失或包含未识别字段')
