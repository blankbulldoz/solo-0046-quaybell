from dataclasses import dataclass

from . import codec, confirmation, inputs
from .errors import invalid


def labels(raw):
    if not isinstance(raw, dict) or len(raw) > 12:
        raise invalid('匹配标签须为对象，最多 12 项')
    return {inputs.name(key, '标签名'): inputs.token(value, '标签值')
            for key, value in raw.items()}


@dataclass(frozen=True)
class Envelope:
    producer: str
    dedup_key: str
    lane: str
    tags: dict
    payload: object
    delay_seconds: int

    @classmethod
    def from_input(cls, raw):
        inputs.require(raw, ('producer','key','lane','payload'),
                       ('tags','delay_seconds', *confirmation.CONTROL_FIELDS))
        result = cls(inputs.name(raw['producer'], '生产者'), inputs.token(raw['key'], '生产键'),
                     inputs.token(raw['lane'], '顺序键'), labels(raw.get('tags', {})),
                     raw['payload'], inputs.integer(raw.get('delay_seconds', 0), 0, 86400, '延迟秒数'))
        if len(result.encoded()) > 16384:
            raise invalid('消息序列化后最多 16 KiB')
        return result

    def encoded(self):
        return codec.pack(dict(lane=self.lane, tags=self.tags, payload=self.payload,
                               delay_seconds=self.delay_seconds))

    def fingerprint(self):
        return codec.fingerprint(self.encoded())
