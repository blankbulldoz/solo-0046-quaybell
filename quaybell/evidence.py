from . import codec, forensics, inspection
from .errors import RelayError


def export_trace(db, message_id, now):
    try:
        facts = inspection.trace(db, message_id)
    except RelayError as exc:
        if exc.status != 404:
            raise
        # Retention may have deleted the live trace while a sealed publication
        # audit receipt remains inside its specified audit period.
        audit = forensics.for_message(db, message_id, now)
        if not audit.get('available'):
            raise
        data = dict(format=forensics.FORMAT, observed_at=now, archived_publication_audit=audit)
        data['sha256'] = codec.fingerprint(codec.pack(data))
        return (codec.pack(data) + '\n').encode('ascii')
    data = dict(format='quaybell-trace/1', observed_at=now, trace=facts)
    data['sha256'] = codec.fingerprint(codec.pack(data))
    return (codec.pack(data) + '\n').encode('ascii')
