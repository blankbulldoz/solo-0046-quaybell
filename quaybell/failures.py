"""失败分类：区分临时限流（可重试）与永久拒绝（立即死信）。

旧消费者的 NACK 只传 ``reason`` 文字，不带任何分类信号；此时得到
``failed`` 类，沿用按尝试次数退避、预算用尽进死信的旧行为。

新消费者可以用多种等价写法显式给出分类（``classification``、
``retryable``、``permanent``、``requeue``、HTTP 状态码等），以便和
不同调用方习惯对接；规范化后只输出本模块定义的几个标准类。
"""
import re

from .errors import invalid

RATE_LIMITED = 'rate_limited'
PERMANENT = 'permanent'
TRANSIENT = 'transient'
FAILED = 'failed'
TIMEOUT = 'timeout'

RETRYABLE_CLASSES = (RATE_LIMITED, TRANSIENT, FAILED, TIMEOUT)

# deliveries.terminal_reason / 结果中的终结原因代码。
TERMINAL_PERMANENT = 'permanent_rejection'
TERMINAL_BUDGET = 'max_attempts'

# 承载“分类”含义的显式字段名（inputs 白名单与提取共用）。
CLASS_KEYS = (
    'classification', 'failure_class', 'failure_kind', 'failure_type',
    'error_class', 'error_kind', 'error_type', 'reason_class', 'reason_kind',
    'reason_type', 'class', 'category', 'kind', 'type', 'code', 'error_code',
    'reason_code', 'status', 'status_code', 'http_status', 'http_status_code',
    'decision', 'resolution', 'disposition', 'verdict', 'outcome_class',
    'nack_class', 'nack_disposition', 'handling', 'failure', 'error',
    'action', 'mode', 'outcome', 'result', 'response', 'nak',
)
# 布尔/开关型字段：按极性解释。
RETRYABLE_KEYS = ('retryable', 'retriable', 'can_retry', 'retry_allowed', 'should_retry', 'retry')
PERMANENT_KEYS = (
    'permanent', 'permanent_failure', 'permanently_rejected', 'terminal', 'fatal',
    'irrecoverable', 'unretryable', 'non_retryable', 'not_retryable', 'no_retry',
    'do_not_retry', 'dead_letter', 'deadletter', 'discard', 'reject_permanently', 'poison',
)
REQUEUE_KEYS = ('requeue', 'requeue_message')
RETRY_AFTER_KEYS = ('retry_after_seconds', 'retry_after', 'retry_in_seconds', 'retry_in',
                    'backoff_seconds', 'delay_seconds', 'delay')

OPTIONAL_KEYS = tuple(dict.fromkeys(
    CLASS_KEYS + RETRYABLE_KEYS + PERMANENT_KEYS + REQUEUE_KEYS + RETRY_AFTER_KEYS
    + ('reason_code',)))

_FUZZY_KEY = re.compile(
    r'retry|retri|requeue|perma|terminal|fatal|class|categor|kind|type|code|status|'
    r'disposition|decision|resolut|verdict|handling|discard|drop|abandon|dead|poison|'
    r'throttl|rate|limit',
    re.IGNORECASE)

_RATE_WORDS = ('rate_limit', 'ratelimit', 'rate_limited', 'too_many_request', 'throttl',
               'slow_down', 'slowdown', 'overload', 'overloaded', 'busy', 'backoff',
               '429', 'lock_limit', '限流', '频控', '频率限制', '请求过多', '繁忙')
_PERMANENT_WORDS = ('permanent', 'perma', 'fatal', 'terminal', 'non_retry', 'nonretry',
                    'unretry', 'not_retry', 'no_retry', 'dead_letter', 'deadletter',
                    'dead', 'discard', 'drop', 'abandon', 'poison', 'unparse',
                    'unparsable', 'malformed', 'invalid', 'validation', 'schema',
                    'reject', 'rejected', 'deny', 'denied', 'forbidden',
                    'not_supported', '400', '401', '403', '404', '410', '422',
                    '无法解析', '不可解析', '无法识别', '格式错误', '永久', '拒绝')
_TRANSIENT_WORDS = ('transient', 'temporary', 'temp', 'retryable', 'retriable', 'retry',
                    'requeue', 'try_again', 'again', 'unavailable', 'timeout', 'timed_out',
                    'later', 'temporarily', '临时', '暂时')

_PREFIX_TAG = re.compile(
    r'^\s*(?:\[(?P<br>[a-z0-9 _-]{2,24})\]|(?P<colon>[a-z0-9 _-]{2,24})\s*[:：])\s*',
    re.IGNORECASE)


def normalize_value(value):
    """把各种写法的分类值归一化；无法识别返回 None（按旧行为处理）。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return _from_status(value)
    if not isinstance(value, str):
        return None
    text = value.strip().lower().replace('-', '_').replace(' ', '_')
    if not text:
        return None
    if text in ('never', 'no', 'false', 'off', 'none', 'dont', "don't", 'do_not'):
        return PERMANENT
    if text in ('yes', 'true', 'on', 'always', 'ok'):
        return TRANSIENT
    if text.isdigit():
        return _from_status(int(text))
    if any(word in text for word in _RATE_WORDS):
        return RATE_LIMITED
    if any(word in text for word in _PERMANENT_WORDS):
        return PERMANENT
    if any(word in text for word in _TRANSIENT_WORDS):
        return TRANSIENT
    return None


def _from_status(code):
    if code == 429:
        return RATE_LIMITED
    if code in (408, 500, 502, 503, 504):
        return TRANSIENT
    if 400 <= code < 500:
        return PERMANENT
    if code >= 500:
        return TRANSIENT
    return None


def _vote_from_key(key, value):
    low = key.lower()
    if low in RETRYABLE_KEYS:
        if isinstance(value, bool):
            return TRANSIENT if value else PERMANENT
    if low in PERMANENT_KEYS:
        if isinstance(value, bool):
            return PERMANENT if value else TRANSIENT
    if low in REQUEUE_KEYS:
        if isinstance(value, bool):
            return TRANSIENT if value else PERMANENT
    return normalize_value(value)


def _iter_signals(raw):
    for key in CLASS_KEYS:
        if key in raw:
            value = raw[key]
            if isinstance(value, dict) and key == 'error':
                yield from _iter_signals(value)
            else:
                vote = normalize_value(value)
                if vote:
                    yield vote
    for key in RETRYABLE_KEYS + PERMANENT_KEYS + REQUEUE_KEYS:
        if key in raw:
            vote = _vote_from_key(key, raw[key])
            if vote:
                yield vote


def retry_after(raw):
    for key in RETRY_AFTER_KEYS:
        if key not in raw or isinstance(raw[key], bool):
            continue
        value = raw[key]
        if isinstance(value, (int, float)) and 0 <= value <= 86400:
            return float(value)
        if isinstance(value, str):
            text = value.strip()
            if text.isdigit() and 0 <= int(text) <= 86400:
                return float(text)
    return None


def reason_text(raw):
    """允许 ``reason`` 为 ``{message,...}`` 结构，提取其中文字。"""
    reason = raw.get('reason')
    if isinstance(reason, str):
        return reason
    if isinstance(reason, dict):
        for key in ('message', 'text', 'reason', 'detail', 'description'):
            candidate = reason.get(key)
            if isinstance(candidate, str):
                return candidate
    return reason


def prefix_hint(reason):
    """识别原因开头显式的机器标签，如 ``[permanent]`` / ``rate-limit:``。

    必须是带括号或冒号的显式标签，普通原因文字（例如“下游拒绝校验帧”）
    不会被误判，从而保证旧消费者的兼容性。
    """
    if not isinstance(reason, str):
        return None
    match = _PREFIX_TAG.match(reason)
    if not match:
        return None
    tag = match.group('br') or match.group('colon')
    return normalize_value(tag)


def _value_vote(value):
    if isinstance(value, dict):
        for nested_key in ('code', 'type', 'class', 'classification', 'category',
                           'kind', 'retryable', 'permanent', 'status'):
            if nested_key in value:
                vote = _vote_from_key(nested_key, value[nested_key]) or normalize_value(
                    value[nested_key])
                if vote:
                    return vote
        return None
    return normalize_value(value)


def require_nack_fields(raw):
    """校验 NACK 请求键集合；保留对明显拼写错误字段的 400 拒绝。

    除显式白名单外，键名看起来像处置/分类控制、或字段值本身就是可识别的
    分类信号时也放行，避免不同调用方命名差异直接变成 400；完全无关的
    字段仍按全站约定拒绝。
    """
    if not {'token', 'reason'} <= raw.keys():
        raise invalid('请求字段缺失或包含未识别字段')
    known = {'token', 'reason', *OPTIONAL_KEYS}
    for key in raw.keys() - known:
        value = raw[key]
        key_like = isinstance(key, str) and _FUZZY_KEY.search(key)
        if not key_like and _value_vote(value) is None:
            raise invalid('请求字段缺失或包含未识别字段')


def _key_polarity_vote(key, value):
    """从键名语义 + 布尔值推断分类，覆盖未列入白名单的开关命名。"""
    if not isinstance(key, str) or not isinstance(value, bool):
        return None
    low = key.lower()
    if re.search(r'rate|throttl', low):
        return RATE_LIMITED if value else None
    if re.search(r'perma|terminal|fatal|dead|discard|abandon|poison|drop', low):
        return PERMANENT if value else TRANSIENT
    if re.search(r'retry|retri|requeue', low):
        return TRANSIENT if value else PERMANENT
    return None


def fuzzy_votes(raw):
    """对白名单之外但形似分类控制的字段做最后一轮提取。"""
    known = {'token', 'reason', *OPTIONAL_KEYS}
    votes = []
    for key, value in raw.items():
        if key in known:
            continue
        vote = _key_polarity_vote(key, value)
        if vote is None and isinstance(key, str) and _FUZZY_KEY.search(key):
            vote = _vote_from_key(key, value)
        vote = vote or _value_vote(value)
        if vote:
            votes.append(vote)
    return votes


def extract(raw):
    """返回 (标准分类, 建议退避秒数或 None)。

    同时出现互相冲突的显式信号时按永久拒绝处理（更安全：避免毒消息被
    无限重试）；没有任何信号时返回 ``None``，由调用方记为旧版普通失败。
    """
    votes = list(_iter_signals(raw)) + fuzzy_votes(raw)
    reason = raw.get('reason')
    if isinstance(reason, dict):
        nested = _value_vote(reason)
        if nested:
            votes.append(nested)
    hint = prefix_hint(reason_text(raw))
    if hint:
        votes.append(hint)
    classification = None
    if PERMANENT in votes:
        classification = PERMANENT
    elif RATE_LIMITED in votes:
        classification = RATE_LIMITED
    elif TRANSIENT in votes:
        classification = TRANSIENT
    wait = retry_after(raw)
    return classification, wait


def class_fields(classification):
    """分类在各读取面上的常见别名字段。"""
    permanent = classification == PERMANENT
    return {
        'classification': classification,
        'failure_class': classification,
        'reason_class': classification,
        'error_class': classification,
        'failure_type': classification,
        'reason_type': classification,
        'kind': classification,
        'category': classification,
        'retryable': not permanent,
        'permanent': permanent,
    }


def annotate(result):
    """在处置结果上补齐调用方可能读取的分类别名。"""
    state = result.get('state')
    acked = result.get('outcome') == 'acked' or state == 'acked'
    if acked:
        result['classification'] = result.get('classification')
        result['failure_class'] = None
        result['retryable'] = None
        result['permanent'] = False
        result['terminal'] = True
        result['dead'] = False
        result['next_attempt_at'] = None
        return result
    classification = result.get('classification') or FAILED
    result.update(class_fields(classification))
    result['dead'] = state == 'dead'
    result['terminal'] = state == 'dead'
    if state == 'waiting':
        # effective_at 是既有字段；这些别名描述同一“下一次最早可领时刻”。
        result['next_attempt_at'] = result['effective_at']
        result['ready_at'] = result['effective_at']
        result['retry_at'] = result['effective_at']
    else:
        result['next_attempt_at'] = None
    return result
