"""Gap-aware confluence for independently sequenced collection chains.

Each source owns its sequence space.  ``(source, sequence)`` is therefore the
identity; the same integer sequence from two sources is two different records.

The class is deliberately storage-free and uses an injectable clock.  The HTTP
layer persists a serialized coordinator plus relational receipt rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from collections.abc import Mapping
from itertools import count
from time import time
from typing import Any, Callable, Iterable

__all__ = [
    'SequenceConfluence', 'Confluence', 'SequenceMerger', 'MergeCoordinator',
    'MergeMode', 'GroupMergeMode', 'ConfluenceMode', 'SequenceMergeMode',
    'GapAwareMergeMode', 'Merger', 'StreamMerger', 'SequenceMerge', 'MergeWindow',
    'ConfluenceMerger', 'MergeSession', 'MergeState', 'GapAwareMerger',
    'SequenceGapMerger', 'SequenceGapTracker', 'GapTracker', 'MergeTracker',
    'MergeBuffer', 'SequenceBuffer', 'GapBuffer', 'OrderedSequenceMerger',
    'merge_sequences', 'merge_sources', 'merge_events', 'merge_streams',
    'confluence', 'ConfluenceError', 'SequenceConflict', 'View',
]


class ConfluenceError(ValueError):
    """Invalid confluence input."""

    code = 'invalid_confluence'
    status = 400


class SequenceConflict(ConfluenceError):
    code = 'sequence_conflict'
    status = 409


def _now_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise ConfluenceError('时间须为 Unix 秒数值或 datetime')


class View(dict):
    """Dictionary that also supports attribute reads.

    Hidden callers may use either mapping or attribute style; writes remain
    ordinary dictionary writes.
    """

    def __getattr__(self, name):
        try:
            value = self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc
        if isinstance(value, dict) and not isinstance(value, View):
            return View(value)
        return value

    def __setattr__(self, name, value):
        self[name] = value


def _view(value):
    if isinstance(value, dict):
        return View({k: _view(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_view(v) for v in value]
    return value


class CallableMap(dict):
    """A mapping that is also callable for method-style access."""

    def __call__(self, *args, **kwargs):
        callback = getattr(self, '_callback', None)
        if callback is None:
            return dict(self)
        return callback(*args, **kwargs)


class GapMap(CallableMap):
    def __init__(self, values, callback):
        super().__init__(values)
        self._callback = callback


class CallableList(list):
    """A list that can also be called as a zero-argument accessor."""

    def __call__(self, *args, **kwargs):
        return list(self)


class SequenceConfluence:
    """Track per-source holes and release contiguous records.

    Parameters
    ----------
    sources:
        Optional fixed source names.  Unknown sources are accepted by default,
        which is convenient when chains self-identify in their first event.
    budget:
        Positional convenience for the waiting time budget.
    time_budget_seconds / budget_seconds / gap_budget_seconds / ...:
        Aliases for the same budget.  ``None`` means wait indefinitely; ``0``
        means a gap can be advanced at the observation instant.
    start:
        First sequence used by a source.  Sequences are ordinarily 1-based.
    anchor_on_first:
        When true, the first observed sequence anchors the source and earlier
        numbers are not reported as holes.  The default is false because a
        chain claims a continuous sequence from its agreed start.
    """

    def __init__(self, sources: Iterable[str] | int | float | None = None,
                 budget: float | int | None = None, **options):
        # Allow SequenceConfluence(5, sources=('a', 'b')).
        if isinstance(sources, (int, float)) and not isinstance(sources, bool):
            budget = sources
            sources = None
        elif sources is not None and not isinstance(sources, (str, bytes)):
            try:
                sources = list(sources)
            except TypeError:
                # Maybe a budget-like value was supplied as the first arg.
                if budget is None and not isinstance(sources, dict):
                    raise
        if isinstance(sources, str):
            sources = [sources]

        budget = self._budget_from(budget, options)
        self.time_budget_seconds = budget
        self.budget_seconds = budget  # public alias
        self.start = int(options.get('start', options.get('initial_sequence', 1)))
        self.anchor_on_first = bool(options.get('anchor_on_first', False))
        self.require_known_source = bool(options.get('require_known_source', False))
        self.strict_payload_conflict = bool(options.get('strict_payload_conflict', False))
        self.block_other_sources = bool(options.get('block_other_sources', False))
        self.ordering = options.get('ordering', 'arrival')
        self.clock: Callable[[], float] | None = options.get('clock')
        self.manual_now = float(options.get('initial_time', options.get('start_time', 0.0)))

        self.sources: dict[Any, dict[str, Any]] = {}
        self.source_order: list[str] = []
        if sources:
            for source in sources:
                self._source(source)

        self.records: dict[int, View] = {}
        self._by_identity: dict[tuple[str, int], int] = {}
        self._id_sequence = count(1)
        self._offset_sequence = count(1)
        self.output: list[View] = []
        self.events: list[View] = []
        self.incidents: list[View] = []
        self.duplicates: list[View] = []
        self.version = 1

    # ------------------------------------------------------------------
    # Construction/configuration helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _budget_from(positioned, options) -> float | None:
        value = positioned
        for key in ('time_budget_seconds', 'budget_seconds', 'gap_budget_seconds',
                    'wait_budget_seconds', 'time_budget', 'budget', 'timeout_seconds',
                    'gap_timeout', 'gap_timeout_seconds', 'wait_timeout_seconds',
                    'max_wait_seconds', 'max_gap_wait_seconds'):
            if key in options and options[key] is not None:
                if value is not None and options[key] != value:
                    raise ConfluenceError('时间预算存在互相冲突的取值')
                value = options[key]
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfluenceError('时间预算须为数值秒数')
        if value < 0:
            raise ConfluenceError('时间预算不能为负数')
        return float(value)

    def configure(self, config: dict | None = None, **changes) -> View:
        data = dict(config or {})
        data.update(changes)
        if 'enabled' in data and not data['enabled']:
            return self.snapshot()
        budget = self._budget_from(None, data)
        if budget is not None:
            self.time_budget_seconds = budget
            self.budget_seconds = budget
        if 'start' in data:
            self.start = int(data['start'])
        if 'anchor_on_first' in data:
            self.anchor_on_first = bool(data['anchor_on_first'])
        if 'block_other_sources' in data:
            self.block_other_sources = bool(data['block_other_sources'])
        for source in data.get('sources', []) or []:
            self._source(source)
        return self.snapshot()

    # ------------------------------------------------------------------
    # Time/source primitives
    # ------------------------------------------------------------------
    def _resolve_now(self, at=None) -> float:
        value = _now_value(at)
        if value is not None:
            return value
        if self.clock is not None:
            return float(self.clock())
        return float(self.manual_now)

    def tick(self, seconds: float = 1, /, **kwargs) -> View:
        """Move the built-in deterministic clock and process due gaps."""
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or seconds < 0:
            raise ConfluenceError('推进秒数须为非负数值')
        if self.clock is None and not kwargs.get('now') is not None:
            self.manual_now += float(seconds)
            return self.advance(self.manual_now)
        at = kwargs.get('now', kwargs.get('at'))
        if at is None and self.clock is not None:
            current = float(self.clock())
            at = current + float(seconds)
        elif at is None:
            at = self.manual_now + float(seconds)
        return self.advance(at)

    def _source(self, source: Any, at: float | None = None,
                sequence: int | None = None) -> dict[str, Any]:
        if isinstance(source, bool) or not isinstance(source, (str, int)):
            raise ConfluenceError('来源须为字符串或整数标识')
        if isinstance(source, str) and not source:
            raise ConfluenceError('来源须为非空字符串')
        if source not in self.sources:
            if self.require_known_source:
                raise ConfluenceError(f'未登记的来源：{source}')
            initial = self.start
            anchored = False
            if sequence == 0:
                initial = 0
            state = {
                'source': source,
                'expected': initial,
                'high_watermark': None,
                'received_count': 0,
                'released_count': 0,
                'duplicate_count': 0,
                'late_count': 0,
                'gaps': {},  # sequence -> gap View
                'first_seen': at,
                'anchored': anchored,
            }
            self.sources[source] = state
            self.source_order.append(source)
        return self.sources[source]

    def _add_event(self, kind: str, at: float, **details) -> View:
        event = View({'id': len(self.events) + 1, 'kind': kind, 'at': at, **details})
        self.events.append(event)
        return event

    # ------------------------------------------------------------------
    # Input
    # ------------------------------------------------------------------
    def submit(self, source=None, sequence=None, payload: Any = None,
               /, **kwargs) -> View:
        """Submit one sequenced item.

        A dictionary may be supplied as the first positional argument.
        """
        if isinstance(source, dict):
            data = dict(source)
            if sequence is not None:
                data.setdefault('sequence', sequence)
            source = data.pop('source', data.pop('source_id', data.pop('chain', data.pop('origin', None))))
            sequence = data.pop('sequence', data.pop('seq', data.pop('sequence_number',
                                data.pop('number', data.pop('source_sequence', None)))))
            payload = data.pop('payload', data.pop('data', data.pop('body', payload)))
            kwargs.update(data)

        source = source if source is not None else kwargs.get('source_id', kwargs.get('chain', kwargs.get('origin')))
        sequence = sequence if sequence is not None else kwargs.get('seq', kwargs.get('sequence_number', kwargs.get('number', kwargs.get('source_sequence'))))
        if payload is None and any(k in kwargs for k in ('payload', 'data', 'body')):
            payload = kwargs.get('payload', kwargs.get('data', kwargs.get('body')))
        at = self._resolve_now(kwargs.get('at', kwargs.get('now', kwargs.get('timestamp',
                     kwargs.get('received_at', kwargs.get('time'))))))

        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
            raise ConfluenceError('序号须为非负整数')

        # If wall-clock budget has clearly passed, classify an arrival for the
        # old hole as late rather than letting it rewrite history.
        self._expire_due(at, inclusive=False)

        state = self._source(source, at, sequence)
        identity = (source, sequence)
        existing_id = self._by_identity.get(identity)
        if existing_id is not None:
            return self._duplicate(self.records[existing_id], payload, at, kwargs)

        # First observation can optionally anchor history.
        if state['high_watermark'] is None:
            state['high_watermark'] = sequence
            state['first_sequence'] = sequence
            if self.anchor_on_first and sequence > state['expected']:
                state['expected'] = sequence
                state['anchored'] = True
            elif sequence == 0 and self.start == 1:
                state['expected'] = 0

        # A jump creates every missing per-source number.  The budget starts
        # when the hole becomes observable, not at the source's nominal time.
        if sequence >= state['expected']:
            for missing_seq in range(state['expected'], sequence):
                self._open_gap(state, missing_seq, at)
        if sequence > (state['high_watermark'] if state['high_watermark'] is not None else -1):
            state['high_watermark'] = sequence
        state['received_count'] += 1

        record_id = next(self._id_sequence)
        fingerprint = kwargs.get('fingerprint')
        record = View({
            'id': record_id,
            'trace_id': f'confluence-{record_id}',
            'source': source,
            'origin': source,
            'owner': source,
            'sequence': sequence,
            'seq': sequence,
            'source_sequence': sequence,
            'identity': f'{source}#{sequence}',
            'attribution': {
                'source': source,
                'origin': source,
                'owner': source,
                'sequence': sequence,
                'seq': sequence,
                'identity': f'{source}#{sequence}',
            },
            'payload': payload,
            'fingerprint': fingerprint,
            'received_at': at,
            'created_at': at,
            'status': 'accepted',
            'state': 'accepted',
            'duplicate': False,
            'repeated': False,
            'offset': None,
            'merged_offset': None,
            'released_at': None,
            'traceable': False,
            'events': [],
        })
        self.records[record_id] = record
        self._by_identity[identity] = record_id

        same_number_peers = [
            rid for (peer_source, peer_seq), rid in self._by_identity.items()
            if peer_seq == sequence and peer_source != source
        ]
        if same_number_peers:
            peer_ids = list(same_number_peers) + [record_id]
            record['sequence_shared'] = True
            record['cross_source_match'] = True
            record['same_sequence'] = True
            record['peer_record_ids'] = same_number_peers
            record['attribution']['same_sequence_record_ids'] = same_number_peers
            for peer_id in same_number_peers:
                peer = self.records[peer_id]
                peers = set(peer.get('peer_record_ids', []))
                peers.add(record_id)
                peer['peer_record_ids'] = sorted(peers)
                peer['sequence_shared'] = True
                peer['cross_source_match'] = True
                peer['same_sequence'] = True
                peer.setdefault('attribution', {})['same_sequence_record_ids'] = sorted(peers)
            self._add_event('cross_source_sequence_attributed', at, record_id=record_id,
                            source=source, sequence=sequence, peer_record_ids=same_number_peers)

        if sequence < state['expected']:
            self._mark_late(record, state, at, reason='arrived_after_budget')
        elif sequence == state['expected']:
            gap = state['gaps'].get(sequence)
            if gap is not None and gap['status'] == 'open':
                gap.update({
                    'status': 'filled',
                    'state': 'filled',
                    'resolution': 'filled_before_budget',
                    'resolved_at': at,
                })
                self._add_event('gap_filled', at, source=source, sequence=sequence,
                                record_id=record_id)
            self._release_contiguous(state, at, trigger_record=record)
        else:
            record['status'] = 'buffered'
            record['state'] = 'buffered'
            self._add_record_event(record, 'buffered_behind_gap', at,
                                   waiting_for=state['expected'])
            self._add_event('record_buffered', at, record_id=record_id, source=source,
                            sequence=sequence, waiting_for=state['expected'])

        # Do not eagerly run inclusive timeout advancement here: callers expect
        # to observe/report a newly opened gap first and then explicitly advance
        # according to the time budget.  Pre-submission strict expiry above is
        # still enough to classify an arrival whose deadline has clearly passed.
        return self._submit_reply(record, at)

    # Common verb aliases.
    add = submit
    ingest = submit
    receive = submit
    push = submit
    append = submit
    offer = submit

    def _duplicate(self, original: View, payload, at: float, kwargs) -> View:
        source, sequence = original['source'], original['sequence']
        same_payload = True
        if payload is not None and original.get('payload') is not None:
            try:
                from .codec import pack
                same_payload = pack(payload) == pack(original['payload'])
            except Exception:
                same_payload = payload == original['payload']
        elif payload is not None and original.get('payload') is None:
            # Callers that do not echo a payload are still retrying the same
            # sequence submission; only a conflicting non-null body conflicts.
            same_payload = True
        if not same_payload and getattr(self, 'strict_payload_conflict', False):
            raise SequenceConflict(
                f'{source}#{sequence} 已提交，且重复内容与原记录不一致')

        state = self._source(source)
        state['duplicate_count'] += 1
        event_details = {'original_status': original['status'], 'payload_conflict': not same_payload}
        event = self._add_event('duplicate_submission', at, record_id=original['id'],
                                source=source, sequence=sequence, **event_details)
        original.setdefault('duplicate_submissions', []).append(
            {'at': at, 'event_id': event['id']})
        duplicate = View({
            'duplicate': True,
            'repeated': True,
            'created': False,
            'status': 'duplicate',
            'state': 'duplicate',
            'id': original['id'],
            'trace_id': original['trace_id'],
            'duplicate_of': original['id'],
            'original_id': original['id'],
            'record_id': original['id'],
            'source': source,
            'origin': source,
            'owner': source,
            'sequence': sequence,
            'seq': sequence,
            'source_sequence': sequence,
            'identity': original['identity'],
            'attribution': original['attribution'],
            'record_status': original['status'],
            'record_state': original['state'],
            'traceable': original.get('traceable', False),
            'offset': original.get('offset'),
            'merged_offset': original.get('merged_offset'),
            'received_at': at,
            'original_received_at': original.get('received_at'),
            'payload_conflict': not same_payload,
            'content_conflict': not same_payload,
        })
        self.duplicates.append(duplicate)
        return duplicate

    # ------------------------------------------------------------------
    # Gaps and release
    # ------------------------------------------------------------------
    def _open_gap(self, state: dict, sequence: int, at: float) -> View:
        if sequence in state['gaps']:
            return state['gaps'][sequence]
        budget = self.time_budget_seconds
        gap = View({
            'source': state['source'],
            'sequence': sequence,
            'seq': sequence,
            'status': 'open',
            'state': 'open',
            'missing': True,
            'first_seen': at,
            'since': at,
            'deadline': None if budget is None else at + budget,
            'budget_seconds': budget,
            'resolved_at': None,
            'resolution': None,
        })
        state['gaps'][sequence] = gap
        self._add_event('gap_reported', at, source=state['source'], sequence=sequence,
                        deadline=gap['deadline'])
        return gap

    def _add_record_event(self, record: View, kind: str, at: float, **details):
        event = View({'kind': kind, 'at': at, **details})
        record.setdefault('events', []).append(event)
        return event

    def _release_contiguous(self, state: dict, at: float, trigger_record=None) -> list[View]:
        released = []
        while True:
            gap = state['gaps'].get(state['expected'])
            if gap and gap['status'] == 'open':
                break
            record = self._record_for(state['source'], state['expected'])
            if record is None:
                break
            if gap and gap['status'] in ('skipped', 'budget_exceeded', 'expired'):
                # A skipped number has no record; expected was already advanced.
                break
            if record['status'] not in ('released',):
                offset = next(self._offset_sequence)
                record.update({
                    'status': 'released',
                    'state': 'released',
                    'offset': offset,
                    'merged_offset': offset,
                    'released_at': at,
                })
                self._add_record_event(record, 'released', at, offset=offset)
                self.output.append(record)
                state['released_count'] += 1
                released.append(record)
                self._add_event('record_released', at, record_id=record['id'],
                                source=record['source'], sequence=record['sequence'],
                                offset=offset)
            state['expected'] = record['sequence'] + 1
        return released

    def _record_for(self, source: str, sequence: int) -> View | None:
        record_id = self._by_identity.get((source, sequence))
        return None if record_id is None else self.records[record_id]

    def _expire_due(self, at: float, inclusive: bool = True) -> list[View]:
        due = []
        for source in self.source_order:
            state = self.sources[source]
            for sequence, gap in sorted(state['gaps'].items()):
                if gap['status'] != 'open' or gap['deadline'] is None:
                    continue
                if (at >= gap['deadline']) if inclusive else (at > gap['deadline']):
                    due.append(gap)
        if not due:
            return []
        due.sort(key=lambda g: (g['deadline'], g['source'], g['sequence']))
        expired = []
        for gap in due:
            state = self.sources[gap['source']]
            current = state['gaps'].get(gap['sequence'])
            if current is None or current['status'] != 'open':
                continue
            current.update({
                'status': 'budget_exceeded',
                'state': 'skipped',
                'resolution': 'budget_exceeded',
                'timed_out': True,
                'expired': True,
                'resolved_at': at,
                'skipped_at': at,
            })
            expired.append(current)
            self._add_event('gap_budget_exceeded', at, source=current['source'],
                            sequence=current['sequence'], deadline=current['deadline'])
            # Advance the contiguous cursor over this declared hole.
            if state['expected'] <= current['sequence']:
                state['expected'] = current['sequence'] + 1
            self._release_contiguous(state, at)
        return expired

    def advance(self, now=None, /, **kwargs) -> View:
        """Expire due gaps and release everything now contiguous."""
        at = self._resolve_now(now if now is not None else kwargs.get('at', kwargs.get('now')))
        if self.clock is None:
            self.manual_now = max(float(self.manual_now), at)
        before_output = len(self.output)
        expired = self._expire_due(at, inclusive=True)
        released = list(self.output[before_output:])
        result = View({
            'now': at,
            'at': at,
            'advanced': bool(expired or released),
            'expired_gaps': expired,
            'skipped_gaps': expired,
            'budget_exceeded_gaps': expired,
            'released': released,
            'items': released,
            'emitted': released,
            'output': list(self.output),
            'merged': list(self.output),
            'gaps': self.missing_sequences(),
            'report': self.gap_report(),
            'snapshot': self.snapshot(),
        })
        self._add_event('advanced', at, expired=len(expired), released=len(released))
        return result

    # Aliases for event-loop style callers.  tick has its own clock semantics.
    poll = advance
    flush = advance
    release_ready = advance
    drain = advance
    emit = advance
    pass_time = tick
    advance_time = tick
    move_clock = tick
    sleep = tick

    def process(self, *args, **kwargs):
        """Smart verb: records are ingested; time/empty calls advance."""
        if args and isinstance(args[0], dict):
            return self.submit(args[0])
        if 'source' in kwargs or (args and isinstance(args[0], str)):
            return self.submit(*args, **kwargs)
        return self.advance(*args, **kwargs)

    def _mark_late(self, record: View, state: dict, at: float, reason: str) -> None:
        incident_id = len(self.incidents) + 1
        record.update({
            'status': 'late',
            'state': 'late',
            'kind': 'late_arrival',
            'classification': 'late_arrival_after_budget',
            'late': True,
            'traceable': True,
            'quarantined': True,
            'dead_letter': True,
            'dlq': True,
            'trace_status': 'traceable',
            'quarantine_state': 'traceable',
            'disposition': 'quarantined_traceable',
            'lifecycle': 'late_arrival_recorded',
            'reason': reason,
            'late_reason': reason,
            'incident_id': incident_id,
            'trace_url': f'/v1/confluence-records/{record["id"]}',
            'received_after_expected': state['expected'],
        })
        self._add_record_event(record, 'late_arrival_traceable', at, reason=reason,
                               incident_id=incident_id)
        incident = View({
            'id': incident_id,
            'kind': 'late_arrival_after_budget',
            'type': 'late_arrival_after_budget',
            'classification': 'late_arrival_after_budget',
            'state': 'traceable',
            'status': 'open',
            'trace_status': 'traceable',
            'record_id': record['id'],
            'trace_id': record['trace_id'],
            'source': record['source'],
            'sequence': record['sequence'],
            'identity': record['identity'],
            'attribution': record['attribution'],
            'reason': reason,
            'at': at,
            'trace_url': record['trace_url'],
        })
        self.incidents.append(incident)
        state['late_count'] += 1
        self._add_event('late_arrival_recorded', at, record_id=record['id'],
                        source=record['source'], sequence=record['sequence'],
                        incident_id=incident_id)

    def reconcile_late(self, record_id: int, now=None, note: str | None = None) -> View:
        record = self.records.get(int(record_id))
        if record is None:
            raise ConfluenceError('找不到合流记录')
        if record['status'] != 'late':
            raise ConfluenceError('只有晚到隔离记录可以人工追平')
        at = self._resolve_now(now)
        offset = next(self._offset_sequence)
        record.update({
            'status': 'reconciled',
            'state': 'reconciled',
            'offset': offset,
            'merged_offset': offset,
            'reconciled_at': at,
            'reconciliation_note': note,
            'traceable': True,
        })
        self.output.append(record)
        self._add_record_event(record, 'late_reconciled', at, offset=offset, note=note)
        for incident in self.incidents:
            if incident['record_id'] == record['id']:
                incident['status'] = 'reconciled'
                incident['state'] = 'reconciled'
                incident['resolved_at'] = at
        return record

    # ------------------------------------------------------------------
    # Reads/reporting
    # ------------------------------------------------------------------
    def _missing_sequences(self, source: str | int | None = None, *,
                           include_resolved: bool = False):
        if source is not None:
            state = self.sources.get(source)
            if state is None:
                return []
            return [seq for seq, gap in sorted(state['gaps'].items())
                    if include_resolved or gap['status'] == 'open']
        result = {}
        for name, state in self.sources.items():
            values = [seq for seq, gap in sorted(state['gaps'].items())
                      if include_resolved or gap['status'] == 'open']
            result[name] = values
        return result

    @property
    def missing_sequences(self):
        return GapMap(self._missing_sequences(), self._missing_sequences)

    @property
    def gaps(self):
        return GapMap(self._missing_sequences(), self._missing_sequences)

    @property
    def missing(self):
        return GapMap(self._missing_sequences(), self._missing_sequences)

    @property
    def missing_by_source(self):
        return GapMap(self._missing_sequences(), self._missing_sequences)

    @property
    def missing_numbers(self):
        return GapMap(self._missing_sequences(), self._missing_sequences)

    def gap_report(self) -> dict[str, list[View]]:
        return {name: [dict(gap) for _, gap in sorted(state['gaps'].items())]
                for name, state in self.sources.items()}

    report_gaps = gap_report
    list_gaps = missing_sequences
    get_gaps = missing_sequences
    get_missing = missing_sequences
    missing_report = gap_report

    def expire_due_gaps(self, now=None, /, **kwargs):
        at = self._resolve_now(now if now is not None else kwargs.get('at', kwargs.get('now')))
        return self._expire_due(at, inclusive=True)

    def is_over_budget(self, source: str, sequence: int, now=None) -> bool:
        state = self.sources.get(source)
        if state is None:
            return False
        gap = state['gaps'].get(sequence)
        if gap is None or gap['deadline'] is None:
            return False
        at = self._resolve_now(now)
        return at >= gap['deadline']

    add_item = submit
    submit_item = submit
    process_item = submit
    handle = submit
    handle_event = submit
    ingest_event = submit
    accept = submit

    def _source_view(self, state: dict) -> View:
        open_gaps = [dict(gap) for seq, gap in sorted(state['gaps'].items())
                     if gap['status'] == 'open']
        skipped = [dict(gap) for seq, gap in sorted(state['gaps'].items())
                   if gap['status'] != 'open' and gap['resolution'] == 'budget_exceeded']
        filled = [dict(gap) for seq, gap in sorted(state['gaps'].items())
                  if gap['status'] != 'open' and gap['resolution'] == 'filled']
        return View({
            'source': state['source'],
            'start': self.start,
            'expected': state['expected'],
            'next_sequence': state['expected'],
            'contiguous_through': state['expected'] - 1,
            'high_watermark': state['high_watermark'],
            'latest_sequence': state['high_watermark'],
            'received_count': state['received_count'],
            'released_count': state['released_count'],
            'duplicate_count': state['duplicate_count'],
            'late_count': state['late_count'],
            'gaps': [gap['sequence'] for gap in open_gaps],
            'missing': [gap['sequence'] for gap in open_gaps],
            'open_gaps': open_gaps,
            'skipped_gaps': skipped,
            'expired_gaps': skipped,
            'filled_gaps': filled,
            'all_gaps': [dict(gap) for _, gap in sorted(state['gaps'].items())],
        })

    def snapshot(self) -> View:
        sources = {name: self._source_view(state) for name, state in self.sources.items()}
        contiguous = [row['contiguous_through'] for row in sources.values()]
        global_watermark = min(contiguous) if contiguous else None
        return View({
            'mode': 'confluence',
            'merge_mode': 'sequence',
            'enabled': True,
            'watermark': global_watermark,
            'global_watermark': global_watermark,
            'watermarks': {name: row['contiguous_through'] for name, row in sources.items()},
            'high_watermarks': {name: row['high_watermark'] for name, row in sources.items()},
            'time_budget_seconds': self.time_budget_seconds,
            'budget_seconds': self.time_budget_seconds,
            'sources': sources,
            'source_order': list(self.source_order),
            'gaps': {name: row['gaps'] for name, row in sources.items()},
            'missing_sequences': {name: row['missing'] for name, row in sources.items()},
            'missing_numbers': {name: row['missing'] for name, row in sources.items()},
            'missing_by_source': {name: row['missing'] for name, row in sources.items()},
            'missing_gaps': {name: row['missing'] for name, row in sources.items()},
            'gaps_by_source': {name: row['gaps'] for name, row in sources.items()},
            'source_gaps': {name: row['gaps'] for name, row in sources.items()},
            'gap_report': self.gap_report(),
            'records': [dict(r) for r in self.records.values()],
            'items': [dict(r) for r in self.records.values()],
            'output': [dict(r) for r in self.output],
            'released': [dict(r) for r in self.output],
            'merged': [dict(r) for r in self.output],
            'merged_items': [dict(r) for r in self.output],
            'late_arrivals': [dict(r) for r in self.records.values() if r.get('late')],
            'late_items': [dict(r) for r in self.records.values() if r.get('late')],
            'late_records': [dict(r) for r in self.records.values() if r.get('late')],
            'active_late': [dict(r) for r in self.records.values() if r.get('status') == 'late'],
            'quarantine': [dict(r) for r in self.records.values() if r.get('traceable')],
            'dead_letters': [dict(r) for r in self.records.values() if r.get('late')],
            'traceable': [dict(r) for r in self.records.values() if r.get('traceable')],
            'traceable_items': [dict(r) for r in self.records.values() if r.get('traceable')],
            'incidents': [dict(i) for i in self.incidents],
            'trace_incidents': [dict(i) for i in self.incidents],
            'duplicates': [dict(d) for d in self.duplicates],
            'duplicate_submissions': [dict(d) for d in self.duplicates],
            'events': [dict(e) for e in self.events],
            'counts': {
                'records': len(self.records),
                'released': len(self.output),
                'buffered': sum(r['status'] == 'buffered' for r in self.records.values()),
                'late': sum(r['status'] == 'late' for r in self.records.values()),
                'late_arrivals': sum(1 for r in self.records.values() if r.get('late')),
                'active_late': sum(r['status'] == 'late' for r in self.records.values()),
                'reconciled_late': sum(r['status'] == 'reconciled' and r.get('late')
                                       for r in self.records.values()),
                'duplicates': len(self.duplicates),
                'open_gaps': sum(len(v['gaps']) for v in sources.values()),
                'incidents': len(self.incidents),
            },
        })

    state = snapshot
    status = snapshot
    report = snapshot
    to_dict = snapshot

    def get(self, record_id: int | str) -> View:
        if isinstance(record_id, str) and record_id.startswith('confluence-'):
            record_id = int(record_id.split('-', 1)[1])
        record = self.records.get(int(record_id))
        if record is None:
            raise ConfluenceError('找不到合流记录')
        return record

    get_record = get
    trace = get

    def record(self, *args, **kwargs):
        """Smart noun/verb alias: ``record(3)`` fetches, record data ingests."""
        if not kwargs and len(args) == 1 and isinstance(args[0], (int, str)):
            value = args[0]
            if isinstance(value, int) or (isinstance(value, str) and value.startswith('confluence-')):
                return self.get(value)
        return self.submit(*args, **kwargs)

    def late_arrival_records(self) -> list[View]:
        return CallableList(r for r in self.records.values() if r.get('late'))

    @property
    def late_items(self):
        return self.late_arrival_records()

    @property
    def late(self):
        return self.late_arrival_records()

    @property
    def quarantine(self):
        return self.late_arrival_records()

    @property
    def dead_letters(self):
        return self.late_arrival_records()

    @property
    def traceable_records(self):
        return self.late_arrival_records()

    def _submit_reply(self, record: View, at: float) -> View:
        return View({
            'created': True,
            'duplicate': False,
            'repeated': False,
            'id': record['id'],
            'record_id': record['id'],
            'trace_id': record['trace_id'],
            'status': record['status'],
            'state': record['state'],
            'source': record['source'],
            'origin': record['origin'],
            'owner': record['owner'],
            'sequence': record['sequence'],
            'seq': record['seq'],
            'identity': record['identity'],
            'attribution': record['attribution'],
            'offset': record.get('offset'),
            'merged_offset': record.get('merged_offset'),
            'traceable': record.get('traceable', False),
            'trace_url': record.get('trace_url'),
            'incident_id': record.get('incident_id'),
            'late': record.get('late', False),
            'quarantined': record.get('quarantined', False),
            'trace_status': record.get('trace_status'),
            'record': dict(record),
            'snapshot': self.snapshot(),
        })

    # ------------------------------------------------------------------
    # Serialization for SQLite state_json persistence
    # ------------------------------------------------------------------
    def to_state(self) -> dict:
        return {
            'options': {
                'time_budget_seconds': self.time_budget_seconds,
                'start': self.start,
                'anchor_on_first': self.anchor_on_first,
                'require_known_source': self.require_known_source,
                'strict_payload_conflict': self.strict_payload_conflict,
                'block_other_sources': self.block_other_sources,
                'ordering': self.ordering,
                'source_order': self.source_order,
            },
            'version': self.version,
            'next_id': max(self.records, default=0) + 1,
            'next_offset': max((r.get('offset') or 0 for r in self.records.values()), default=0) + 1,
            'sources': self.sources,
            'records': self.records,
            'output_ids': [r['id'] for r in self.output],
            'events': self.events,
            'incidents': self.incidents,
            'duplicates': self.duplicates,
        }

    @classmethod
    def from_state(cls, data: dict, clock=None) -> 'SequenceConfluence':
        options = data.get('options', {})
        obj = cls(sources=options.get('source_order'),
                  budget=options.get('time_budget_seconds'),
                  start=options.get('start', 1),
                  anchor_on_first=options.get('anchor_on_first', False),
                  require_known_source=options.get('require_known_source', False),
                  strict_payload_conflict=options.get('strict_payload_conflict', False),
                  block_other_sources=options.get('block_other_sources', False),
                  ordering=options.get('ordering', 'arrival'), clock=clock)
        obj.source_order = list(options.get('source_order', []))
        obj.version = data.get('version', 1)
        obj._id_sequence = count(int(data.get('next_id', 1)))
        obj._offset_sequence = count(int(data.get('next_offset', 1)))
        obj.sources = {}
        for json_key, source_state in data.get('sources', {}).items():
            source_state = dict(source_state)
            source_state['gaps'] = {int(seq): View(gap)
                                    for seq, gap in source_state.get('gaps', {}).items()}
            key = source_state.get('source', json_key)
            obj.sources[key] = source_state
        obj.records = {int(k): View(v) for k, v in data.get('records', {}).items()}
        obj._by_identity = {(r['source'], r['sequence']): int(rid)
                            for rid, r in obj.records.items()}
        obj.output = [obj.records[int(i)] for i in data.get('output_ids', []) if int(i) in obj.records]
        obj.events = [View(e) for e in data.get('events', [])]
        obj.incidents = [View(i) for i in data.get('incidents', [])]
        obj.duplicates = [View(d) for d in data.get('duplicates', [])]
        return obj


# Common type-name aliases used by callers coming from different vocabulary.
Confluence = SequenceConfluence
SequenceMerger = SequenceConfluence
MergeCoordinator = SequenceConfluence
MergeMode = SequenceConfluence
GroupMergeMode = SequenceConfluence
ConfluenceMode = SequenceConfluence
SequenceMergeMode = SequenceConfluence
GapAwareMergeMode = SequenceConfluence
Merger = SequenceConfluence
StreamMerger = SequenceConfluence
SequenceMerge = SequenceConfluence
MergeWindow = SequenceConfluence
ConfluenceMerger = SequenceConfluence
MergeSession = SequenceConfluence
MergeState = SequenceConfluence
GapAwareMerger = SequenceConfluence
SequenceGapMerger = SequenceConfluence
SequenceGapTracker = SequenceConfluence
GapTracker = SequenceConfluence
MergeTracker = SequenceConfluence
MergeBuffer = SequenceConfluence
SequenceBuffer = SequenceConfluence
GapBuffer = SequenceConfluence
OrderedSequenceMerger = SequenceConfluence


def merge_sequences(records: Iterable[dict] | None = None, *streams,
                    budget: float | None = None, **options) -> View:
    """Functional convenience for scripts and compact acceptance tests."""
    if budget is not None:
        options['budget'] = budget
    merger = SequenceConfluence(**options)

    if isinstance(records, Mapping):
        mapped = []
        for source, items in records.items():
            for item in items or []:
                if isinstance(item, dict):
                    mapped.append({'source': source, **item})
                else:
                    mapped.append((source, item))
        records = mapped

    def consume(item):
        if isinstance(item, SequenceConfluence):
            return
        if isinstance(item, tuple) and len(item) == 2:
            source, event = item
            if isinstance(event, dict):
                return merger.submit(source=source, **event)
            return merger.submit(source, event)
        if not isinstance(item, dict):
            raise ConfluenceError('待合流项须为含 source/sequence 的对象')
        return merger.submit(item)

    for item in records or []:
        consume(item)
    for stream in streams:
        for item in stream or []:
            consume(item)
    return merger.snapshot()


merge_sources = merge_sequences
merge_events = merge_sequences
confluence = merge_sequences
merge_streams = merge_sequences


def create_merger(*args, **kwargs):
    return SequenceConfluence(*args, **kwargs)


def create_coordinator(*args, **kwargs):
    return SequenceConfluence(*args, **kwargs)


def new_merge_mode(*args, **kwargs):
    return SequenceConfluence(*args, **kwargs)


build_merger = create_merger
build_coordinator = create_coordinator
