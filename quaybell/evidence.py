from . import codec, inspection, investigation


def _sealed(data):
    sealed = dict(data)
    sealed['sha256'] = codec.fingerprint(codec.pack(sealed))
    return (codec.pack(sealed) + '\n').encode('ascii')


def export_trace(db, message_id, now):
    facts = inspection.trace(db, message_id)
    return _sealed({'format': 'quaybell-trace/1', 'observed_at': now, 'trace': facts})


def export_handoff(db, delivery_id, now):
    return _sealed(investigation.handoff(db, delivery_id, now))
