from .codec import pack, unpack


def append(db, channel, now, kind, details, message_id=None):
    db.execute('INSERT INTO journal(channel,message_id,at,kind,details) VALUES (?,?,?,?,?)',
               (channel, message_id, now, kind, pack(details)))


def events_for(db, message_id):
    rows = db.execute('SELECT id,at,kind,details FROM journal WHERE message_id=? ORDER BY id',
                      (message_id,))
    return [dict(row) | {'details': unpack(row['details'])} for row in rows]


def recorded_redrive(db, message_id, source_id):
    '''读取重投提交时写下的判断；重复重投和事后核对都以它为准，不按当前队列重算。'''
    rows = db.execute("SELECT id,at,details FROM journal WHERE message_id=? AND kind='redriven' ORDER BY id",
                      (message_id,))
    for row in rows:
        details = unpack(row['details'])
        if details.get('source_id') == source_id:
            return {'event_id': row['id'], 'at': row['at'], 'details': details}
    return None
