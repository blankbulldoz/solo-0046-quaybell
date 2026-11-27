from . import clockwork
from .codec import pack, unpack


def append(db, channel, now, kind, details, message_id=None):
    db.execute('INSERT INTO journal(channel,message_id,at,kind,details) VALUES (?,?,?,?,?)',
               (channel, message_id, now, kind, pack(details)))


def events_for(db, message_id):
    rows = db.execute('SELECT id,at,kind,details FROM journal WHERE message_id=? ORDER BY id',
                      (message_id,))
    return [clockwork.event(dict(row) | {'details': unpack(row['details'])}) for row in rows]
