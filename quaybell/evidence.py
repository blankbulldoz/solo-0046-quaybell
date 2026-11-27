from . import clockwork, codec, inspection


def export_trace(db, message_id, now):
    facts = inspection.trace(db, message_id)
    # 证据合同 quaybell-trace/1 仍绑定 Unix 秒观察时间；now 为内部整数毫秒。
    data = dict(format='quaybell-trace/1', observed_at=clockwork.to_seconds(now), trace=facts)
    data['sha256'] = codec.fingerprint(codec.pack(data))
    return (codec.pack(data) + '\n').encode('ascii')
