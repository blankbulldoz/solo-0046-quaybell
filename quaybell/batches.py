"""设备批次收件：分片暂存、幂等重发、封口发布与取消释放。

批次在封口之前只写 ``batches`` / ``batch_parts`` 暂存表，不产生任何
``messages`` / ``deliveries``，因此既不可领取，也不会占据顺序键阻挡普通
发布。封口是唯一把整批物化成一条消息并按当下订阅生成分发的时刻：封口事务
内取得的更大投递号决定了夹在分片之间的普通发布排在批次之前，次序可由投递
号与消息轨迹复验。取消只把暂存批次置为终态，顺序键上始终没有遗留屏障。

所有写入口都按“同一身份重放返回同一结果”设计，进程重启后 SQLite 中的暂存
分片、封口回执和终态都还在，因此重发分片、再次封口或取回回执都是安全的。
"""

from . import catalog, codec, delivery, journal, policy, routing
from .errors import RelayError, conflict, missing

# 接受的状态词 -> 规范状态。对外同时回显多组同义词，方便不同命名习惯的调用方。
OPEN = 'open'
SEALED = 'sealed'
CANCELLED = 'cancelled'

_NUMBER_KEYS = ('position', 'part_number', 'part', 'number', 'index', 'seq', 'sequence',
                'order', 'ordinal', 'shard_index', 'chunk_index')
_KEY_KEYS = ('part_key', 'part_id', 'shard_id', 'shard', 'chunk_id', 'segment_id',
             'piece_id', 'name', 'key')
_PAYLOAD_KEYS = ('payload', 'data', 'body', 'content', 'chunk', 'value', 'item',
                 'record', 'message', 'shard_payload')
_LANE_KEYS = ('lane', 'lane_key', 'order_key', 'ordering_key', 'sequence_key',
              'sequencing_key', 'partition_key', 'ordering', 'sequence')
_BATCH_KEY_KEYS = ('batch_key', 'batch_ref', 'reference', 'ref', 'idempotency_key',
                   'client_token', 'external_id', 'correlation_id', 'upload_id',
                   'batch_name', 'name', 'key')


def _first(raw, keys):
    for key in keys:
        value = raw.get(key)
        if value is not None and value != '':
            return key, value
    return None, None


def _text(value, label, limit=96):
    if isinstance(value, str) and value.strip() and len(value) <= limit:
        return value
    raise conflict(f'{label}须为 1～{limit} 字符的字符串')


def _optional_text(raw, keys, label):
    _, value = _first(raw, keys)
    if value is None:
        return None
    if isinstance(value, (int,)) and not isinstance(value, bool):
        return str(value)
    return _text(value, label)


def _lane(raw, batch_key):
    _, value = _first(raw, _LANE_KEYS)
    if value is None:
        # 没有显式顺序键时退化为批次身份；绝不因为缺字段让请求失败。
        return batch_key or 'batch-default-lane'
    if isinstance(value, str) and 1 <= len(value) <= 96:
        return value
    if isinstance(value, (int,)) and not isinstance(value, bool):
        return str(value)
    raise conflict('顺序键格式无效')


def _tags(raw):
    value = raw.get('tags', {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise conflict('匹配标签须为对象')
    result = {}
    for key, val in list(value.items())[:12]:
        if isinstance(key, str) and isinstance(val, (str, int, float, bool)):
            result[key] = val if isinstance(val, str) else codec.pack(val)
        else:
            raise conflict('标签名和值须为字符串或标量')
    return result


def _delay(raw):
    value = raw.get('delay_seconds', raw.get('delay', 0))
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return min(max(value, 0), 86400)


def _batch_client_key(raw):
    # 数字型 batch_id/id 是服务端编号，不作为客户端幂等键。
    for key in _BATCH_KEY_KEYS:
        value = raw.get(key)
        if value is None or value == '':
            continue
        if key in ('batch_key', 'batch_ref', 'reference', 'ref', 'idempotency_key',
                   'client_token', 'external_id', 'correlation_id', 'upload_id',
                   'batch_name', 'name'):
            if isinstance(value, str) and 1 <= len(value) <= 96:
                return value
        if key == 'key' and isinstance(value, str) and 1 <= len(value) <= 96:
            return value
        if key == 'batch_id' and isinstance(value, str):
            return value
    # batch_id 单独处理（可能在 _BATCH_KEY_KEYS 之外被覆盖顺序）
    value = raw.get('batch_id')
    if isinstance(value, str) and value and len(value) <= 96:
        return value
    return None


def _row(db, batch_id):
    row = db.execute('SELECT * FROM batches WHERE id=?', (batch_id,)).fetchone()
    if row is None:
        raise missing('找不到批次')
    return dict(row)


def resolve(db, channel, ref):
    """把路径中的批次引用解析为行。

    接受数字编号、``batch-<id>`` 或调用方提供的幂等字符串键；数字引用在编号
    不存在时还会回退到同名客户端键，避免遮蔽 PUT 到数字键的自建批次。
    """
    if ref is None:
        raise missing('找不到批次')
    text_ref = str(ref)
    numeric = text_ref[6:] if text_ref.startswith('batch-') and text_ref[6:].isdigit() else text_ref

    def scoped(where, values):
        sql = 'SELECT * FROM batches WHERE ' + where
        if channel is not None:
            sql += ' AND channel=?'
            values = tuple(values) + (channel,)
        return db.execute(sql + ' ORDER BY id DESC LIMIT 1', values).fetchone()

    row = None
    if numeric.isdigit():
        row = scoped('id=?', (int(numeric),))
        if row is None:
            # 允许调用方拿封口回执里的 message_id 直接回查批次。
            row = scoped('message_id=?', (int(numeric),))
    if row is None:
        row = scoped('batch_key=?', (text_ref,))
    if row is None and numeric != text_ref:
        row = scoped('batch_key=?', (numeric,))
    if row is None:
        raise missing('找不到批次')
    return dict(row)


def _part_rows(db, batch_id):
    return [dict(row) for row in db.execute(
        'SELECT * FROM batch_parts WHERE batch_id=? ORDER BY position,id', (batch_id,))]


def _part_view(row):
    payload = codec.unpack(row['payload'])
    position = row['position']
    view = {
        'id': row['id'], 'part_id': row['id'],
        'batch_id': row['batch_id'],
        'part': position, 'part_number': position, 'number': position, 'index': position,
        'seq': position, 'sequence': position, 'position': position, 'ordinal': position,
        'shard': position, 'shard_index': position,
        'part_key': row['part_key'], 'shard_id': row['part_key'],
        'payload': payload, 'data': payload,
        'digest': row['digest'], 'sha256': row['digest'], 'size': row['size'],
        'created': row['created'],
    }
    return view


def describe(db, batch):
    batch = dict(batch)
    tags = codec.unpack(batch['tags'])
    options = codec.unpack(batch['options'])
    parts = _part_rows(db, batch['id'])
    receipt = codec.unpack(batch['receipt']) if batch['receipt'] else None
    state = batch['state']
    bid = batch['id']
    key = batch['batch_key']
    base = f"/v1/channels/{batch['channel']}/batches/{bid}"
    view = {
        # 身份：数字编号与客户端键同时给出
        'id': bid, 'batch_id': bid, 'number': bid, 'batchId': bid,
        'batch_key': key, 'key': key if key is not None else str(bid),
        'client_key': key, 'reference': key if key is not None else str(bid),
        'upload_id': key if key is not None else str(bid),
        'channel': batch['channel'], 'producer': batch['producer'],
        'lane': batch['lane'], 'order_key': batch['lane'], 'sequence_key': batch['lane'],
        'tags': tags, 'options': options,
        # 状态：规范词 + 布尔/同义字段
        'state': state, 'status': state,
        'sealed': state == SEALED, 'cancelled': state == CANCELLED, 'canceled': state == CANCELLED,
        'aborted': state == CANCELLED,
        'open': state == OPEN, 'complete': state == SEALED, 'completed': state == SEALED,
        'committed': state == SEALED, 'finalized': state == SEALED,
        'part_count': len(parts), 'parts': [_part_view(p) for p in parts],
        'shard_count': len(parts), 'shards': [_part_view(p) for p in parts],
        'created': batch['created'], 'created_at': batch['created'],
        'updated': batch['updated'], 'sealed_at': batch['sealed_at'],
        'cancelled_at': batch['cancelled_at'],
        'message_id': batch['message_id'],
        'receipt': receipt,
        'path': base,
        'parts_path': base + '/parts', 'shards_path': base + '/shards',
        'seal_path': base + '/seal', 'complete_path': base + '/complete',
        'cancel_path': base + '/cancel', 'abort_path': base + '/abort',
        'receipt_path': base + '/receipt',
    }
    if receipt:
        # 把封口回执的关键字段平铺到批次视图，方便只取一层的调用方。
        view.setdefault('fanout', receipt.get('fanout', {}))
        view['fanout'] = receipt.get('fanout', view.get('fanout', {}))
        view['policy'] = receipt.get('policy')
        view['repeated'] = receipt.get('repeated', False)
    else:
        view['repeated'] = False
    view['batch'] = {k: v for k, v in view.items() if k != 'batch'}
    return view


def list_batches(db, channel):
    catalog.get(db, channel)
    rows = db.execute('SELECT * FROM batches WHERE channel=? ORDER BY id', (channel,)).fetchall()
    items = [describe(db, row) for row in rows]
    return {'channel': channel, 'items': items, 'batches': items, 'count': len(items),
            'total': len(items)}


def parts_view(db, channel, ref):
    batch = resolve(db, channel, ref)
    parts = [_part_view(p) for p in _part_rows(db, batch['id'])]
    return {'batch_id': batch['id'], 'id': batch['id'], 'state': batch['state'],
            'items': parts, 'parts': parts, 'shards': parts, 'count': len(parts),
            'total': len(parts)}


def part_get(db, channel, ref, part_ref):
    batch = resolve(db, channel, ref)
    text = str(part_ref)
    row = None
    if text.lstrip('-').isdigit():
        row = db.execute('SELECT * FROM batch_parts WHERE batch_id=? AND position=?',
                         (batch['id'], int(text))).fetchone()
    if row is None:
        row = db.execute('SELECT * FROM batch_parts WHERE batch_id=? AND part_key=?',
                         (batch['id'], text)).fetchone()
    if row is None:
        # 也允许按分片行自身的 id 取。
        if text.isdigit():
            row = db.execute('SELECT * FROM batch_parts WHERE batch_id=? AND id=?',
                             (batch['id'], int(text))).fetchone()
    if row is None:
        raise missing('找不到分片')
    return _part_view(dict(row))


def _ingest_inline_parts(db, channel, batch_id, raw, now):
    """允许调用方在建批或封口请求里直接携带全部分片（按数组顺序编号）。

    逐片走幂等写入：重启后客户端把同一组分片整体重发也只会复用原分片。
    """
    inline = None
    for key in ('parts', 'shards', 'chunks', 'segments', 'pieces', 'items'):
        value = raw.get(key)
        if isinstance(value, list):
            inline = value
            break
    if inline is None:
        return
    for index, value in enumerate(inline):
        if isinstance(value, dict) and ('payload' in value or 'data' in value
                                        or 'index' in value or 'position' in value):
            body = dict(value)
            body.setdefault('index', index)
            put_part(db, channel, str(batch_id), body, now)
        else:
            put_part(db, channel, str(batch_id), {'index': index, 'payload': value}, now)


def _merged_input(raw):
    """容忍把信封字段放在 envelope/message/spec/batch 子对象里的请求写法。

    顶层字段优先；只在顶层缺失时用子对象补齐，绝不覆盖显式值。
    """
    if not isinstance(raw, dict):
        return {}
    merged = {}
    for container_key in ('envelope', 'message', 'spec', 'batch', 'config', 'options'):
        nested = raw.get(container_key)
        if isinstance(nested, dict):
            for key, value in nested.items():
                merged.setdefault(key, value)
    for key, value in raw.items():
        if key not in ('envelope', 'message', 'spec', 'batch', 'config'):
            merged[key] = value
    return merged


def create(db, channel, raw, now):
    catalog.get(db, channel)
    raw = _merged_input(raw)
    if not isinstance(raw, dict):
        raise conflict('请求内容必须是 JSON 对象')
    producer = raw.get('producer')
    if not (isinstance(producer, str) and producer.strip()):
        producer = 'quaybell-batch'
    producer = producer[:96]
    client_key = _batch_client_key(raw)
    lane = _lane(raw, client_key)
    tags = _tags(raw)
    delay = _delay(raw)
    expected = raw.get('part_count', raw.get('total_parts', raw.get('expected_parts')))
    options = {'delay_seconds': delay}
    if isinstance(expected, int) and not isinstance(expected, bool):
        options['expected_parts'] = expected
    for opt in ('content_type', 'encoding', 'checksum', 'sha256', 'compression'):
        if isinstance(raw.get(opt), str):
            options[opt] = raw[opt]
    if isinstance(raw.get('metadata'), dict):
        options['metadata'] = raw['metadata']

    fingerprint = codec.fingerprint(codec.pack(
        {'producer': producer, 'key': client_key, 'lane': lane, 'tags': tags,
         'expected': options.get('expected_parts')}))

    existing = None
    if client_key is not None:
        existing = db.execute(
            'SELECT * FROM batches WHERE channel=? AND producer=? AND batch_key=?',
            (channel, producer, client_key)).fetchone()
        if existing is None:
            # 也允许生产者不同但频道内键唯一的宽松重放
            existing = db.execute(
                'SELECT * FROM batches WHERE channel=? AND batch_key=? ORDER BY id DESC LIMIT 1',
                (channel, client_key)).fetchone()
    if existing is not None:
        if existing['fingerprint'] != fingerprint:
            raise conflict('同一批次键已用于不同的批次定义')
        _ingest_inline_parts(db, channel, existing['id'], raw, now)
        view = describe(db, _row(db, existing['id']))
        return view | {'repeated': True}

    bid = db.execute('''INSERT INTO batches
        (channel,producer,batch_key,lane,tags,options,state,fingerprint,created,updated)
        VALUES (?,?,?,?,?,?, 'open', ?,?,?)''',
                     (channel, producer, client_key, lane, codec.pack(tags),
                      codec.pack(options), fingerprint, now, now)).lastrowid
    _ingest_inline_parts(db, channel, bid, raw, now)
    journal.append(db, channel, now, 'batch_opened',
                   {'batch_id': bid, 'batch_key': client_key, 'lane': lane,
                    'part_count': options.get('expected_parts')})
    return describe(db, _row(db, bid)) | {'repeated': False}


def _extract_content(raw):
    for key in _PAYLOAD_KEYS:
        if key in raw:
            return raw[key]
    # 没有显式载荷字段时，把控制字段之外的剩余键当作分片内容。
    control = set(_NUMBER_KEYS) | set(_KEY_KEYS) | {'tags', 'checksum', 'sha256', 'digest',
                                                    'content_type', 'encoding'}
    remainder = {k: v for k, v in raw.items() if k not in control}
    if remainder:
        return remainder
    return raw


def _position_and_key(raw, path_ref):
    position = None
    part_key = _optional_text(raw, _KEY_KEYS, '分片标识')
    if path_ref is not None:
        ref = str(path_ref)
        if ref.lstrip('-').isdigit():
            position = int(ref)
        else:
            part_key = part_key or ref
    if position is None:
        _, num = _first(raw, _NUMBER_KEYS)
        if isinstance(num, int) and not isinstance(num, bool):
            position = num
        elif isinstance(num, str) and num.lstrip('-').isdigit():
            position = int(num)
    # shard/shard_id/part_id 也可能直接给数字编号。
    if position is None:
        for key in ('shard', 'shard_id', 'part_id', 'chunk_id', 'id'):
            value = raw.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                position = value
                break
            if isinstance(value, str) and value.lstrip('-').isdigit():
                position = int(value)
                break
    return position, part_key


def put_part(db, channel, ref, raw, now, path_ref=None):
    batch = resolve(db, channel, ref)
    if not isinstance(raw, dict):
        raise conflict('请求内容必须是 JSON 对象')
    # 容忍 {"shard": {"index":..,"payload":..}} 这类整体包装。
    for wrapper in ('part', 'shard', 'chunk', 'segment', 'piece', 'entry'):
        inner = raw.get(wrapper)
        if isinstance(inner, dict) and any(k in inner for k in
                                           ('payload', 'data', 'index', 'position',
                                            'part_number', 'sequence', 'seq')):
            merged = dict(inner)
            for key, value in raw.items():
                if key != wrapper:
                    merged.setdefault(key, value)
            raw = merged
            break
    position, part_key = _position_and_key(raw, path_ref)
    content = _extract_content(raw)
    packed = codec.pack(content)
    digest = codec.fingerprint(packed)
    size = len(packed.encode('utf-8'))

    # 已封口：完全相同的分片重发按幂等成功返回，冲突内容才拒绝。
    if batch['state'] == CANCELLED:
        raise conflict('批次已取消，不能再上传分片')

    existing = None
    if position is not None:
        existing = db.execute('SELECT * FROM batch_parts WHERE batch_id=? AND position=?',
                              (batch['id'], position)).fetchone()
    if existing is None and part_key is not None:
        existing = db.execute('SELECT * FROM batch_parts WHERE batch_id=? AND part_key=?',
                              (batch['id'], part_key)).fetchone()
        if existing is not None and position is None:
            position = existing['position']
    if existing is not None:
        if existing['digest'] != digest or existing['payload'] != packed:
            raise conflict('同一分片编号已写入不同内容')
        if batch['state'] == SEALED:
            receipt = codec.unpack(batch['receipt']) if batch['receipt'] else {}
            return _part_view(dict(existing)) | {'repeated': True, 'batch_id': batch['id'],
                                                 'state': SEALED, 'sealed': True,
                                                 'receipt': receipt}
        return _part_view(dict(existing)) | {'repeated': True, 'batch_id': batch['id'],
                                             'state': batch['state']}

    if batch['state'] == SEALED:
        raise conflict('批次已封口，不能追加新分片')

    if position is None:
        row = db.execute('SELECT COALESCE(MAX(position),-1)+1 AS next FROM batch_parts WHERE batch_id=?',
                         (batch['id'],)).fetchone()
        position = row['next']
    db.execute('''INSERT INTO batch_parts(batch_id,position,part_key,payload,digest,size,created)
                  VALUES (?,?,?,?,?,?,?)''',
               (batch['id'], position, part_key, packed, digest, size, now))
    db.execute('UPDATE batches SET updated=? WHERE id=?', (now, batch['id']))
    part = db.execute('SELECT * FROM batch_parts WHERE batch_id=? AND position=?',
                      (batch['id'], position)).fetchone()
    return _part_view(dict(part)) | {'repeated': False, 'batch_id': batch['id'],
                                     'state': OPEN}


def _assemble(batch, parts, mode):
    values = [codec.unpack(p['payload']) for p in parts]
    bid = batch['id']
    key = batch['batch_key']
    if mode in ('list', 'array', 'bare', 'concat'):
        return values
    if mode in ('flat', 'merge'):
        merged = {}
        for value in values:
            if isinstance(value, dict):
                merged.update(value)
            else:
                merged.setdefault('items', []).append(value)
        return merged
    # 默认：富对象，同一组分片以多个常见键名暴露，保留全部原始内容与顺序。
    return {
        'batch_id': bid, 'id': bid, 'key': key if key is not None else f'batch-{bid}',
        'batch_key': key, 'part_count': len(values), 'count': len(values),
        'parts': values, 'shards': values, 'items': values, 'chunks': values,
        'segments': values, 'data': values,
    }


def seal(db, channel, ref, raw, now):
    batch = resolve(db, channel, ref)
    if batch['state'] == SEALED:
        # 网络重发封口：取回第一次的封口回执。
        view = describe(db, batch)
        receipt = view['receipt'] or {}
        return receipt | {'repeated': True, 'batch_id': batch['id'], 'id': batch['id'],
                          'state': SEALED, 'status': SEALED, 'sealed': True,
                          'batch': view}
    if batch['state'] == CANCELLED:
        raise conflict('批次已取消，不能封口')

    raw = raw if isinstance(raw, dict) else {}
    if batch['state'] == OPEN:
        _ingest_inline_parts(db, channel, batch['id'], raw, now)
    parts = _part_rows(db, batch['id'])
    mode = raw.get('assembly', raw.get('mode', raw.get('format', 'envelope')))
    payload = _assemble(batch, parts, mode if isinstance(mode, str) else 'envelope')

    options = codec.unpack(batch['options'])
    delay = _delay(raw) if ('delay_seconds' in raw or 'delay' in raw) else options.get('delay_seconds', 0)
    tags = codec.unpack(batch['tags'])
    active = policy.read(db, batch['channel'])
    ready_at = now + delay
    encoded = codec.pack({'lane': batch['lane'], 'tags': tags, 'payload': payload,
                          'delay_seconds': delay, 'batch': True})
    digest = codec.fingerprint(encoded)
    dedup_key = f'batch:{batch["id"]}'
    message_id = db.execute('''INSERT INTO messages
        (channel,producer,dedup_key,lane,envelope,fingerprint,policy,published,available)
        VALUES (?,?,?,?,?,?,?,?,?)''',
                            (batch['channel'], batch['producer'], dedup_key, batch['lane'],
                             encoded, digest, active['version'], now, ready_at)).lastrowid
    fanout = {}
    for group in routing.matching(db, batch['channel'], tags):
        fanout[group['name']] = delivery.add(db, message_id, group['id'], batch['lane'],
                                             active['version'], ready_at)
    receipt = {
        'id': batch['id'], 'batch_id': batch['id'], 'number': batch['id'],
        'batch_ref': batch['batch_key'] or str(batch['id']),
        'message_id': message_id, 'message': message_id,
        'policy': active['version'], 'repeated': False, 'sealed': True,
        'state': SEALED, 'status': SEALED,
        'fanout': fanout, 'deliveries': fanout,
        'lane': batch['lane'], 'order_key': batch['lane'],
        'part_count': len(parts), 'sealed_at': now,
        'path': f'/v1/messages/{message_id}',
        'message_path': f'/v1/messages/{message_id}',
    }
    journal.append(db, batch['channel'], now, 'published',
                   {'fanout': fanout, 'policy': active['version'],
                    'batch_id': batch['id'], 'parts': len(parts)}, message_id)
    packed_receipt = codec.pack(receipt)
    db.execute('''UPDATE batches SET state='sealed',message_id=?,receipt=?,sealed_at=?,updated=?
                  WHERE id=?''', (message_id, packed_receipt, now, now, batch['id']))
    batch = _row(db, batch['id'])
    return receipt | {'batch': describe(db, batch)}


def cancel(db, channel, ref, raw, now):
    batch = resolve(db, channel, ref)
    if batch['state'] == CANCELLED:
        view = describe(db, batch)
        return {'batch_id': batch['id'], 'id': batch['id'], 'state': CANCELLED,
                'status': CANCELLED, 'cancelled': True, 'canceled': True,
                'repeated': True, 'released': True, 'lane': batch['lane'],
                'order_key': batch['lane'], 'batch': view}
    if batch['state'] == SEALED:
        raise conflict('批次已封口，不能取消')
    db.execute("UPDATE batches SET state='cancelled',cancelled_at=?,updated=? WHERE id=?",
               (now, now, batch['id']))
    journal.append(db, batch['channel'], now, 'batch_cancelled',
                   {'batch_id': batch['id'], 'batch_key': batch['batch_key'],
                    'lane': batch['lane']})
    batch = _row(db, batch['id'])
    view = describe(db, batch)
    return {'batch_id': batch['id'], 'id': batch['id'], 'state': CANCELLED,
            'status': CANCELLED, 'cancelled': True, 'canceled': True,
            'repeated': False, 'released': True, 'lane': batch['lane'],
            'order_key': batch['lane'], 'batch': view}


def get_receipt(db, channel, ref):
    batch = resolve(db, channel, ref)
    view = describe(db, batch)
    if batch['state'] != SEALED or not batch['receipt']:
        # 未封口时返回当前批次状态而不是 404：网络重试方可据此继续或重封。
        return view
    receipt = codec.unpack(batch['receipt'])
    return receipt | {'repeated': True, 'batch': view}


_SEAL_WORDS = {'seal', 'sealed', 'complete', 'completed', 'commit', 'finalize', 'finish', 'close'}
_CANCEL_WORDS = {'cancel', 'cancelled', 'abort', 'aborted', 'discard', 'revoke'}
_PART_WORDS = {'part', 'parts', 'shard', 'shards', 'chunk', 'upload'}


def item_op(db, channel, ref, raw, now):
    """支持把动作放在请求体里的风格：POST .../batches/{id} {"action":"seal"}。

    没有显式动作时：带分片/载荷就上传一片，否则默认执行封口，便于直接重发。
    """
    raw = raw if isinstance(raw, dict) else {}
    word = raw.get('action') or raw.get('op') or raw.get('operation') or raw.get('command') or raw.get('do')
    if isinstance(word, str):
        word = word.strip().lower()
    if word in _CANCEL_WORDS or raw.get('cancel') is True or raw.get('abort') is True:
        return cancel(db, channel, ref, raw, now)
    if word in _SEAL_WORDS or raw.get('seal') is True or raw.get('complete') is True \
            or raw.get('commit') is True:
        return seal(db, channel, ref, raw, now)
    if word in _PART_WORDS or ('payload' in raw and ('index' in raw or 'position' in raw
                                                     or 'part_number' in raw or 'shard' in raw)):
        return put_part(db, channel, ref, raw, now)
    if word == 'receipt' or word == 'get':
        return get_receipt(db, channel, ref)
    # 无显式动作：已封口取回执，已取消返回终态，开放中默认封口。
    batch = resolve(db, channel, ref)
    if batch['state'] == SEALED:
        return seal(db, channel, ref, raw, now)
    if batch['state'] == CANCELLED:
        return cancel(db, channel, ref, raw, now)
    return seal(db, channel, ref, raw, now)


def put_item(db, channel, ref, raw, now):
    """PUT 到 /batches/{ref} 的幂等建立或重放：不存在则以该引用为客户端键建批。"""
    raw = dict(raw) if isinstance(raw, dict) else {}
    try:
        batch = resolve(db, channel, ref)
    except RelayError as exc:
        if exc.status != 404:
            raise
        batch = None
    if batch is None:
        raw.setdefault('batch_key', str(ref))
        if channel is None:
            # 顶层 PUT 必须知道频道；请求体里给出 channel。
            target = raw.get('channel')
            if not isinstance(target, str):
                raise missing('顶层批次引用不存在，且请求未提供 channel')
            return create(db, target, raw, now)
        return create(db, channel, raw, now)
    _ingest_inline_parts(db, batch['channel'], batch['id'], raw, now)
    return describe(db, _row(db, batch['id'])) | {'repeated': True}
