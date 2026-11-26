from . import codec, inspection


def export_trace(db, message_id, now):
    facts = inspection.trace(db, message_id, now)
    data = dict(format='quaybell-trace/1', observed_at=now, trace=facts)
    data['sha256'] = codec.fingerprint(codec.pack(data))
    return (codec.pack(data) + '\n').encode('ascii')
