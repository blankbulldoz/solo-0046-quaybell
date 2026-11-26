"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging

from quaybell.codec import fingerprint, pack
from quaybell.evidence_stream import IncompleteEvidence, verify_legacy, verify_prefix, verify_stream

from .session import CHANNEL, GROUP, POLICY, envelope, session


def _frame_starts(raw):
    starts = []
    position = 0
    marker = b'{"format"'
    while True:
        position = raw.find(marker, position)
        if position < 0:
            return starts
        starts.append(position)
        position += len(marker)


def channel(client):
    record = client.channel()
    client.request('/channels', {'name': 'wire-gate'}, 409)
    assert record['policy']['version'] == 1
    return record


def revision(client):
    client.channel()
    client.subscribe()
    old = client.publish()
    data = POLICY | {'expected': 1, 'lease_seconds': 12}
    record = client.request(CHANNEL + '/policy', data, 201)
    client.request(CHANNEL + '/policy', data, 409)
    new = client.publish('new-signal', 'coil-south')
    first, second = client.claim(2)
    assert (old['policy'], new['policy']) == (1, 2)
    assert second['lease']['expires'] - first['lease']['expires'] == 8
    return {'active': record['version'], 'old_lease': first['lease']['expires'], 'new_lease': second['lease']['expires']}


def group(client):
    client.channel()
    client.publish('early')
    client.subscribe(tags={'feed': 'samples'})
    hit = client.publish('matched')
    miss = client.publish('wrong', tags={'feed': 'alerts'})
    assert list(hit['fanout']) == ['oscillator'] and miss['fanout'] == {}
    assert client.claim()[0]['message_id'] == hit['message_id']
    return {'matched': hit['fanout'], 'unmatched': miss['fanout']}


def preview(client):
    client.channel()
    client.subscribe(tags={'feed': 'samples'})
    request = envelope()
    candidate = client.request(CHANNEL + '/preview', request)
    client.request(CHANNEL + '/preview', request | {'tags': {'feed': []}}, 400)
    assert candidate['groups'] == ['oscillator']
    assert client.request(CHANNEL)['messages'] == []
    return candidate


def publish(client):
    client.channel()
    client.subscribe()
    first = client.publish(payload={'second': 2, 'first': 1})
    client.subscribe('after-image')
    same = client.publish(expected=200, payload={'first': 1, 'second': 2})
    client.publish(expected=409, payload={'first': 2, 'second': 2})
    assert same == first | {'repeated': True}
    return same


def inspect(client):
    client.channel()
    client.subscribe()
    sent = client.publish()
    taken = client.claim()[0]
    client.settle(taken)
    trace = client.request(f"/messages/{sent['message_id']}")
    client.request('/messages/99999', expected=404)
    assert [event['kind'] for event in trace['events']] == ['published', 'leased', 'acked']
    assert taken['lease']['token'] not in pack(trace)
    return trace


def pause(client):
    client.channel()
    client.subscribe()
    client.publish()
    client.publish('second', 'coil-south')
    taken = client.claim()[0]
    client.request(GROUP + '/pause', {'paused': True})
    assert client.claim() == [] and client.settle(taken)['state'] == 'acked'
    client.request(GROUP + '/pause', {'paused': False})
    return {'resumed_delivery': client.claim()[0]['delivery_id']}


def claim(client):
    client.channel()
    client.subscribe()
    head = client.publish(delay_seconds=4)
    tail = client.publish('tail')
    independent = client.publish('aside', 'coil-south')
    first = client.claim(3)
    assert [item['message_id'] for item in first] == [independent['message_id']]
    client.tick(4)
    next_item = client.claim()[0]
    assert next_item['message_id'] == head['message_id']
    client.settle(next_item)
    assert client.claim()[0]['message_id'] == tail['message_id']
    return {'initial': independent['message_id'], 'head': head['message_id'], 'after_ack': tail['message_id']}


def ack(client):
    client.channel()
    client.subscribe()
    client.publish()
    taken = client.claim()[0]
    a = client.settle(taken)
    b = client.settle(taken)
    client.settle(taken | {'lease': {'token': 'stale-lease'}}, expected=409)
    assert b == a | {'repeated': True}
    return b


def retry(client):
    client.channel()
    client.subscribe()
    client.publish()
    first = client.claim()[0]
    next_time = client.settle(first, 'nack')['effective_at']
    assert client.claim() == []
    client.tick(3)
    second = client.claim()[0]
    assert second['lease']['number'] == 2 and second['lease']['token'] != first['lease']['token']
    client.settle(first, expected=409)
    return {'next_time': next_time, 'final': client.settle(second, 'nack')}


def sweep(client):
    client.channel()
    client.subscribe()
    client.publish()
    item = client.claim()[0]
    client.tick(4)
    client.settle(item, expected=409)
    client.tick(3)
    result = client.request('/sweep', {'limit': 1})
    assert result['recovered'][0]['effective_at'] == item['lease']['expires'] + 3
    assert client.claim()[0]['lease']['number'] == 2
    return result


def dead(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.subscribe('witness')
    item = client.publish()
    client.request(f"/deliveries/{item['fanout']['witness']}/redrive", {}, 409)
    client.settle(client.claim()[0], 'nack')
    rows = client.request(CHANNEL + '/dead')['items']
    assert len(rows) == 1 and rows[0]['group_name'] == 'oscillator'
    assert rows[0]['last_attempt']['outcome'] == 'nacked'
    return rows[0]


def redrive(client):
    first = dead(client)
    client.request(CHANNEL + '/policy', POLICY | {'expected': 1}, 201)
    path = f"/deliveries/{first['id']}/redrive"
    new = client.request(path, {}, 201)
    assert client.request(path, {}) == new | {'repeated': True}
    assert client.claim()[0]['policy'] == 2
    return new


def board(client):
    client.channel()
    client.subscribe()
    client.publish(delay_seconds=20)
    client.publish('tail')
    client.publish('ready', 'coil-south')
    old = client.request(CHANNEL)['groups'][0]
    client.request(GROUP + '/pause', {'paused': True})
    now = client.request(CHANNEL)['groups'][0]
    assert old['counts'] == dict(blocked=1, delayed=1, ready=1)
    assert now['counts'] == dict(blocked=1, paused=2)
    return {'first': old['counts'], 'after_pause': now['counts']}


def retention(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    first = client.publish()
    later = client.publish('stay', 'coil-south')
    client.settle(client.claim()[0], 'nack')
    root = first['fanout']['oscillator']
    child = client.request(f'/deliveries/{root}/redrive', {}, 201)
    picked = client.claim(2)
    assert {row['delivery_id'] for row in picked} == {child['delivery_id'], later['fanout']['oscillator']}
    client.settle(next(row for row in picked if row['delivery_id'] == child['delivery_id']))
    client.tick(11)
    plan = client.request(CHANNEL + '/retention')
    assert [row['id'] for row in plan['eligible']] == [first['message_id']]
    done = client.request(CHANNEL + '/retention', {})
    client.request(f"/messages/{first['message_id']}", expected=404)
    assert done['removed'] == [first['message_id']] and client.publish()['message_id'] > later['message_id']
    return done


def export(client):
    facts = inspect(client)
    record = client.request(f"/messages/{facts['message']['id']}/evidence")
    digest = record.pop('sha256')
    assert record['trace'] == facts and fingerprint(pack(record)) == digest
    return {'format': record['format'], 'sha256': digest}


def _line_frames(raw):
    return [line for line in raw.split(b'\n') if line]


def stream_evidence(client):
    """Cross-check legacy and streamed evidence for one full lineage."""
    client.channel(POLICY)  # max_attempts=2：先超时退避，再拒绝用尽预算
    client.subscribe('oscillator')
    client.subscribe('witness', tags={'feed': 'samples'})
    sent = client.publish()
    message_id = sent['message_id']
    groups = sorted(sent['fanout'])
    assert groups == ['oscillator', 'witness']

    # 第一次领取后让租约自然到期：产生一个 expired 尝试并按截止时间退避。
    first = client.claim(worker='bench-a')[0]
    client.tick(first['lease']['expires'] - client.clock[0])
    client.settle(first, expected=409)
    sweep = client.request('/sweep', {})
    expired_attempt = sweep['recovered'][0]
    assert expired_attempt['outcome'] == 'expired'
    backoff_until = expired_attempt['effective_at']
    assert backoff_until == first['lease']['expires'] + POLICY['retry_seconds']

    # 退避结束再领取并拒绝，预算用尽后进入死信，再追加一个重投后继。
    client.tick(backoff_until - client.clock[0])
    second = client.claim(worker='bench-b')[0]
    assert second['delivery_id'] == first['delivery_id'] and second['lease']['number'] == 2
    client.settle(second, 'nack')
    dead_rows = client.request(CHANNEL + '/dead')['items']
    assert len(dead_rows) == 1
    root_id = dead_rows[0]['id']
    client.request(CHANNEL + '/policy', POLICY | {'expected': 1, 'lease_seconds': 12}, 201)
    redriven = client.request(f'/deliveries/{root_id}/redrive', {}, 201)
    successor_id = redriven['delivery_id']
    assert redriven['policy'] == 2

    facts = client.request(f'/messages/{message_id}')

    # 旧版单对象证据：新接收端仍要能核对历史导出。
    status, headers, legacy_raw = client.download(f'/messages/{message_id}/evidence')
    assert status == 200 and headers['Content-Type'].startswith('application/json')
    legacy = verify_legacy(legacy_raw)
    assert legacy['format'] == 'quaybell-trace/1'
    assert legacy['trace'] == facts

    # 新版逐行 JSON 证据：与旧包同一读快照（期间无写入），各带自身观察时间。
    status, headers, stream_raw = client.download(
        f'/messages/{message_id}/evidence/stream', accept='application/x-ndjson')
    assert status == 200 and headers['Content-Type'].startswith('application/x-ndjson')
    stream = verify_stream(_line_frames(stream_raw))
    assert isinstance(stream['observed_at'], (int, float))
    assert stream['observed_at'] >= legacy['observed_at']  # 不同时间语义：各自绑定导出时刻
    assert stream['trace'] == legacy['trace']
    assert stream['sha256'] != legacy['sha256']

    # 身份对照：消息、两个消费组、原死信与重投后继的身份在两包中一致。
    deliveries = stream['trace']['deliveries']
    assert {row['group_name'] for row in deliveries} == {'oscillator', 'witness'}
    root = next(row for row in deliveries if row['id'] == root_id)
    assert root['group_name'] == 'oscillator' and root['state'] == 'dead'
    successor = next(row for row in deliveries if row['parent_id'] == root_id)
    assert successor['id'] == successor_id and successor['policy'] == 2
    assert successor['group_name'] == 'oscillator'

    # 事件顺序对照：发布、领取、超时、再领取、拒绝、重投按日志次序排列。
    kinds = [event['kind'] for event in stream['trace']['events']]
    assert kinds == ['published', 'leased', 'expired', 'leased', 'nacked', 'redriven']
    assert kinds == [event['kind'] for event in legacy['trace']['events']]

    # 时间语义对照：到期尝试以租约截止计时，明确拒绝以处置时间计时。
    attempts = root['attempts']
    expired_row = next(row for row in attempts if row['outcome'] == 'expired')
    nacked_row = next(row for row in attempts if row['outcome'] == 'nacked')
    assert [row['number'] for row in attempts] == [1, 2]
    assert expired_row['finished'] >= expired_row['expires']
    assert backoff_until == expired_row['expires'] + POLICY['retry_seconds']
    assert nacked_row['started'] == backoff_until
    assert successor['ready_at'] == nacked_row['finished']

    # 中断识别：在事实段内部切断，保证信封完好且缺少结束帧。
    starts = _frame_starts(stream_raw)
    target = (starts[2] + starts[4]) // 2
    next_newline = stream_raw.index(b'\n', target)
    cut = target + (next_newline - target) // 2
    _, _, partial_raw = client.download(f'/messages/{message_id}/evidence/stream', cut_after=cut)
    assert len(partial_raw) < len(stream_raw) and not partial_raw.endswith(b'\n')
    full_lines = _line_frames(stream_raw)
    try:
        verify_stream(_line_frames(partial_raw))
    except IncompleteEvidence:
        pass
    else:
        raise AssertionError('截断的证据流未被识别为未完成')
    prefix = verify_prefix(_line_frames(partial_raw))
    assert prefix['complete'] is False
    assert isinstance(prefix['observed_at'], (int, float))
    assert prefix['observed_at'] >= stream['observed_at']
    assert prefix['sections']  # 至少有一个事实段通过链式摘要核对
    assert len(prefix['sha256']) == 64 and 0 < prefix['frames_seen'] < len(full_lines)
    for section in prefix['sections']:
        assert prefix['trace'][section] == legacy['trace'][section]
    # 用完整包的前 N 帧独立重算，摘要与事实必须与中断包观察点一致。
    anchor = verify_prefix(full_lines[:prefix['frames_seen']])
    assert anchor['sections'] == prefix['sections']
    assert anchor['sha256'] == prefix['sha256']
    assert anchor['trace'] == prefix['trace']
    observation_frames = prefix['frames_seen']

    # 拉流过程中新增一次尝试：已下载的包仍可独立核对到它自己的观察点。
    before = verify_stream(_line_frames(stream_raw))
    client.claim(worker='bench-c')  # 后继被领取，写入新的 leased 事件与尝试
    after_facts = client.request(f'/messages/{message_id}')
    _, _, after_raw = client.download(f'/messages/{message_id}/evidence/stream',
                                      accept='application/x-ndjson')
    after = verify_stream(_line_frames(after_raw))
    assert before['trace'] == legacy['trace'] == facts
    assert (len(after['trace']['events']) == len(before['trace']['events']) + 1
            and after['trace']['events'][-1]['kind'] == 'leased')
    assert after['sha256'] != before['sha256']
    assert after['observed_at'] >= after_facts['events'][-1]['at']

    return {'format': 'quaybell-trace-stream/1', 'groups': groups,
            'events': kinds, 'stream_sha256': stream['sha256'],
            'legacy_sha256': legacy['sha256'], 'verified_prefix_frames': observation_frames,
            'observation_points': [before['observed_at'], after['observed_at']]}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export,
             stream_evidence=stream_evidence)


def main():
    parser = argparse.ArgumentParser(description='运行一个有界的真实中继工作流')
    parser.add_argument('workflow', choices=FLOWS.keys())
    args = parser.parse_args()
    logging.getLogger().setLevel(logging.WARNING)
    with session() as client:
        result = FLOWS[args.workflow](client)
    print(f'{args.workflow} 通过 {pack(result)}')


if __name__ == '__main__':
    main()
