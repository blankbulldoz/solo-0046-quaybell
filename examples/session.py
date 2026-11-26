from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json

from quaybell.server import RelayServer
from quaybell.persistence import RelayPersistence

CHANNEL = '/channels/wire-gate'
GROUP = CHANNEL + '/groups/oscillator'
POLICY = dict(lease_seconds=4, retry_seconds=3, max_attempts=2, retention_seconds=11)


class Session:
    def __init__(self, server, clock):
        self.base = f'http://127.0.0.1:{server.server_port}/v1'
        self.clock = clock

    def request(self, path, data=None, expected=200):
        info = {} if data is None else dict(data=json.dumps(data).encode('utf-8'),
                                             headers={'Content-Type': 'application/json'})
        req = Request(self.base + path, **info)
        try:
            response = urlopen(req, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            body = json.load(response)
            if response.code != expected:
                raise RuntimeError((path, response.code, expected, body))
            return body

    def tick(self, amount):
        self.clock[0] += amount

    def channel(self, policy=None):
        return self.request('/channels', {'name': 'wire-gate', 'policy': policy or POLICY}, 201)

    def subscribe(self, name='oscillator', tags=None):
        return self.request(CHANNEL + '/groups', {'name': name, 'tags': tags or {}}, 201)

    def revise_rules(self, name='oscillator', tags=None, expected=201):
        return self.request(CHANNEL + f'/groups/{name}/rules', {'tags': tags or {}}, expected)

    def shadow(self, name='oscillator'):
        return self.request(CHANNEL + f'/groups/{name}/shadow')

    def shadow_start(self, candidate, tags, name='oscillator', expected=201, **extra):
        return self.request(CHANNEL + f'/groups/{name}/shadow',
                            {'candidate': candidate, 'tags': tags} | extra, expected)

    def shadow_switch(self, candidate, tags, name='oscillator', expected=201):
        return self.request(CHANNEL + f'/groups/{name}/shadow/candidate',
                            {'candidate': candidate, 'tags': tags}, expected)

    def shadow_stop(self, name='oscillator', expected=200):
        return self.request(CHANNEL + f'/groups/{name}/shadow/stop', {}, expected)

    def shadow_evidence(self, name='oscillator'):
        return self.request(CHANNEL + f'/groups/{name}/shadow/evidence')

    def publish(self, key='signal-1', lane='coil-north', expected=201, **extra):
        return self.request(CHANNEL + '/messages', envelope(key, lane, **extra), expected)

    def claim(self, limit=1, worker='bench-worker'):
        return self.request(GROUP + '/claim', {'worker': worker, 'limit': limit})['items']

    def settle(self, item, mode='ack', expected=200):
        data = {'token': item['lease']['token']}
        if mode == 'nack':
            data['reason'] = '下游拒绝校验帧'
        return self.request(f"/deliveries/{item['delivery_id']}/{mode}", data, expected)


def envelope(key='signal-1', lane='coil-north', **extra):
    return dict(producer='signal-source', key=key, lane=lane, payload={'value': 18},
                tags={'feed': 'samples'}) | extra


@contextmanager
def session():
    with TemporaryDirectory(prefix='quaybell-') as directory:
        clock = [1_730_000_000.0]
        persistence = RelayPersistence(Path(directory) / 'relay.sqlite3')
        with RelayServer(('127.0.0.1', 0), persistence, lambda: clock[0]) as server:
            thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
            thread.start()
            try:
                yield Session(server, clock)
            finally:
                server.shutdown()
                thread.join()
