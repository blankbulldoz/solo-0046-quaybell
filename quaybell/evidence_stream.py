"""Versioned, line-delimited evidence exchange.

The legacy ``quaybell-trace/1`` export is one JSON object with a single
content digest.  External auditors consume JSON line by line, so the
``quaybell-trace-stream/1`` package frames the same snapshot as NDJSON: an
opening envelope frame, one frame per fact section, a closing frame and an
explicit end frame.  Every frame carries its ordinal and a digest chained
over the canonical bytes of every frame seen so far, so an independent
reader can verify the package up to any observation point and recognise a
truncated download.
"""
from . import codec, inspection

STREAM_FORMAT = 'quaybell-trace-stream/1'
LEGACY_FORMAT = 'quaybell-trace/1'
STREAM_MEDIA = 'application/x-ndjson; charset=utf-8'
SECTIONS = ('message', 'deliveries', 'events')


def _line(value):
    return codec.pack(value) + '\n'


def stream_trace(db, message_id, now):
    """Build canonical NDJSON frames for one read-snapshot of a message.

    The snapshot and every frame are computed eagerly while the database
    session is open; the returned iterable only replays the frozen bytes so
    the HTTP layer can stream them without holding a connection.
    """
    facts = inspection.trace(db, message_id)
    chain = b''
    ordinal = 0
    frames = []

    def emit(frame):
        nonlocal chain, ordinal
        ordinal += 1
        record = {'format': STREAM_FORMAT, 'ordinal': ordinal, 'frame': frame}
        chain = codec.fingerprint(chain + _line(record).encode('utf-8')).encode('ascii')
        record['sha256'] = chain.decode('ascii')
        frames.append(_line(record).encode('ascii'))
    emit({'type': 'envelope', 'observed_at': now, 'message_id': message_id,
          'section_count': len(SECTIONS)})
    for section in SECTIONS:
        emit({'type': 'section', 'section': section, 'data': facts[section]})
    emit({'type': 'close', 'observed_at': now})
    emit({'type': 'end'})
    return iter(frames)


class IncompleteEvidence(Exception):
    """Raised when a package cannot be verified or ends before its end frame."""


def _record(raw_line, partial_allowed):
    try:
        record = codec.unpack(raw_line)
    except ValueError:
        # A half-written trailing line is the signature of an interrupted
        # download; malformed whole frames in the middle are still rejected.
        if partial_allowed:
            return None
        raise IncompleteEvidence('证据帧不是合法 JSON，下载可能被截断') from None
    if not isinstance(record, dict):
        raise IncompleteEvidence('证据帧必须是 JSON 对象')
    return record


def _scan(lines):
    """Walk frames, validating the chained digest and frame grammar.

    Returns the full verification state, including whether the terminal end
    frame was reached, so callers can distinguish a tampered package from a
    merely truncated one and still trust the frames before the cut.
    """
    chain = ''
    ordinal = 0
    observed_at = None
    sections = []
    facts = {}
    phase = 'envelope'
    complete = False
    pending = [raw for raw in lines if (raw.decode('utf-8') if isinstance(raw, (bytes, bytearray))
                                        else raw).strip()]
    last_index = len(pending) - 1
    for index, raw_line in enumerate(pending):
        text = raw_line.decode('utf-8') if isinstance(raw_line, (bytes, bytearray)) else raw_line
        record = _record(text.rstrip('\n'), partial_allowed=index == last_index)
        if record is None:
            # Trailing half frame: the download stopped mid-line; trust only
            # the complete frames already chained and report it as unfinished.
            break
        digest = record.pop('sha256', None)
        if record.get('format') != STREAM_FORMAT:
            raise IncompleteEvidence('证据流格式标识无法识别')
        ordinal += 1
        if record.get('ordinal') != ordinal:
            raise IncompleteEvidence('证据帧序号不连续，下载可能被截断')
        chain = codec.fingerprint(chain.encode('ascii') + _line(record).encode('utf-8'))
        if not isinstance(digest, str) or digest != chain:
            raise IncompleteEvidence('证据链摘要不匹配，包内容无法独立核对')
        frame = record.get('frame')
        if not isinstance(frame, dict):
            raise IncompleteEvidence('证据帧缺少载荷')
        kind = frame.get('type')
        if kind == 'envelope':
            if phase != 'envelope':
                raise IncompleteEvidence('证据信封必须是首帧')
            observed_at = frame.get('observed_at')
            if frame.get('message_id') is None or not isinstance(observed_at, (int, float)):
                raise IncompleteEvidence('证据流信封字段不完整')
            phase = 'section'
        elif kind == 'section':
            if phase != 'section' or frame.get('section') not in SECTIONS:
                raise IncompleteEvidence('证据段顺序不合法')
            sections.append(frame['section'])
            facts[frame['section']] = frame['data']
            if len(sections) == len(SECTIONS):
                phase = 'close'
        elif kind == 'close':
            if phase != 'close' or frame.get('observed_at') != observed_at:
                raise IncompleteEvidence('证据收尾帧与快照观察时间不一致')
            phase = 'end'
        elif kind == 'end':
            if phase != 'end' or sections != list(SECTIONS):
                raise IncompleteEvidence('证据流在结束前缺少事实段')
            complete = True
        else:
            raise IncompleteEvidence(f'未知证据帧类型 {kind!r}')
    return {'complete': complete, 'observed_at': observed_at, 'sections': sections,
            'facts': facts, 'sha256': chain, 'frames_seen': ordinal}


def verify_stream(lines):
    """Consume a complete NDJSON package and return its reconstructed facts.

    Each frame's chained digest is recomputed from the canonical bytes of all
    preceding frames.  A missing or mismatched digest, a gap in the ordinals,
    a wrong frame order or an absent ``end`` frame means the download did not
    finish.  The returned mapping mirrors the legacy object export so both
    packages can be compared by identity and event order.
    """
    state = _scan(lines)
    if not state['complete']:
        raise IncompleteEvidence('证据流缺少结束帧，下载未完成')
    return {'format': LEGACY_FORMAT, 'observed_at': state['observed_at'],
            'trace': state['facts'], 'sha256': state['sha256']}


def verify_prefix(lines):
    """Verify a possibly interrupted package up to its last whole frame.

    Every frame up to the cut is independently checked against the digest
    chain, so the result states exactly which observation point the partial
    download can be trusted to.  ``complete`` is false when the end frame has
    not arrived; ``sections`` names the fact sections already received.
    """
    state = _scan(lines)
    return {'format': STREAM_FORMAT, 'complete': state['complete'],
            'observed_at': state['observed_at'], 'sections': state['sections'],
            'frames_seen': state['frames_seen'], 'sha256': state['sha256'],
            'trace': state['facts'] if state['sections'] else {}}


def verify_legacy(raw):
    """Validate an already exported ``quaybell-trace/1`` object download."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode('utf-8')
    record = codec.unpack(raw)
    if not isinstance(record, dict) or record.get('format') != LEGACY_FORMAT:
        raise IncompleteEvidence('旧版证据格式标识无法识别')
    digest = record.get('sha256')
    without_digest = {key: value for key, value in record.items() if key != 'sha256'}
    if not isinstance(digest, str) or codec.fingerprint(codec.pack(without_digest)) != digest:
        raise IncompleteEvidence('旧版证据摘要不匹配')
    return record
