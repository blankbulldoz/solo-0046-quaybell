from dataclasses import dataclass

from . import codec, inputs
from .errors import invalid


def labels(raw):
    if not isinstance(raw, dict) or len(raw) > 12:
        raise invalid('匹配标签须为对象，最多 12 项')
    return {inputs.name(key, '标签名'): inputs.token(value, '标签值')
            for key, value in raw.items()}


def source_value(raw):
    value = raw.get('source', raw.get('source_id', raw.get('chain', raw.get('origin'))))
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise invalid('来源须为字符串或整数标识')
    if isinstance(value, str) and not 1 <= len(value) <= 64:
        raise invalid('来源须为 1～64 字符')
    return value


def sequence_value(raw):
    value = None
    for key in ('sequence', 'seq', 'sequence_number', 'source_sequence'):
        if key in raw and raw[key] is not None:
            value = raw[key]
            break
    if value is None:
        return None
    return inputs.integer(value, 0, 2**63 - 2, '来源序号')


@dataclass(frozen=True)
class Envelope:
    producer: str
    dedup_key: str
    lane: str
    tags: dict
    payload: object
    delay_seconds: int
    source: str | int | None = None
    sequence: int | None = None

    @classmethod
    def from_input(cls, raw):
        inputs.require(raw, ('producer','key','lane','payload'),
                       ('tags','delay_seconds','source','source_id','chain','origin',
                        'sequence','seq','sequence_number','source_sequence'))
        source = source_value(raw)
        sequence = sequence_value(raw)
        if (source is None) != (sequence is None):
            raise invalid('合流来源 source 与序号 sequence 必须同时提供')
        result = cls(inputs.name(raw['producer'], '生产者'), inputs.token(raw['key'], '生产键'),
                     inputs.token(raw['lane'], '顺序键'), labels(raw.get('tags', {})),
                     raw['payload'], inputs.integer(raw.get('delay_seconds', 0), 0, 86400, '延迟秒数'),
                     source, sequence)
        if len(result.encoded()) > 16384:
            raise invalid('消息序列化后最多 16 KiB')
        return result

    def encoded(self):
        value = dict(lane=self.lane, tags=self.tags, payload=self.payload,
                     delay_seconds=self.delay_seconds)
        if self.source is not None:
            value['source'] = self.source
        if self.sequence is not None:
            value['sequence'] = self.sequence
        return codec.pack(value)
