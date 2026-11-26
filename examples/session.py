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
    def __init__(self, server, clock, db_path=None, persistence=None):
        self.server = server
        self.clock = clock
        self.db_path = Path(db_path) if db_path else None
        self.persistence = persistence
        self.base = f'http://127.0.0.1:{server.server_port}/v1'
        self._thread = None

    def attach_thread(self, thread):
        self._thread = thread

    def request(self, path, data=None, expected=200, method=None):
        info = {} if data is None else dict(data=json.dumps(data).encode('utf-8'),
                                             headers={'Content-Type': 'application/json'})
        req = Request(self.base + path, method=method, **info)
        try:
            response = urlopen(req, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            body = json.load(response)
            # 默认期望 200 的只读/动作调用兼容任意 2xx（新建资源可能返回 201）。
            acceptable = response.code == expected or (expected == 200 and 200 <= response.code < 300)
            if not acceptable:
                raise RuntimeError((path, response.code, expected, body))
            return body

    def tick(self, amount):
        self.clock[0] += amount

    def channel(self, policy=None):
        return self.request('/channels', {'name': 'wire-gate', 'policy': policy or POLICY}, 201)

    def subscribe(self, name='oscillator', tags=None):
        return self.request(CHANNEL + '/groups', {'name': name, 'tags': tags or {}}, 201)

    def publish(self, key='signal-1', lane='coil-north', expected=201, **extra):
        return self.request(CHANNEL + '/messages', envelope(key, lane, **extra), expected)

    def claim(self, limit=1, worker='bench-worker'):
        return self.request(GROUP + '/claim', {'worker': worker, 'limit': limit})['items']

    def settle(self, item, mode='ack', expected=200):
        data = {'token': item['lease']['token']}
        if mode == 'nack':
            data['reason'] = '下游拒绝校验帧'
        return self.request(f"/deliveries/{item['delivery_id']}/{mode}", data, expected)

    # ---- 批次收件辅助 ----
    def create_batch(self, key='device-batch', lane='coil-north', expected=201, **extra):
        body = {'producer': 'signal-source', 'key': key, 'lane': lane} | extra
        return self.request(CHANNEL + '/batches', body, expected)

    def put_shard(self, batch, index, payload=None, expected=201, **extra):
        bid = batch['batch_id'] if isinstance(batch, dict) else batch
        body = {'index': index, 'payload': payload if payload is not None else {'index': index}} | extra
        return self.request(f'{CHANNEL}/batches/{bid}/shards', body, expected)

    def seal_batch(self, batch, expected=201, **extra):
        bid = batch['batch_id'] if isinstance(batch, dict) else batch
        return self.request(f'{CHANNEL}/batches/{bid}/seal', extra or {}, expected)

    def cancel_batch(self, batch, expected=200):
        bid = batch['batch_id'] if isinstance(batch, dict) else batch
        return self.request(f'{CHANNEL}/batches/{bid}/cancel', {}, expected)

    def batch_receipt(self, batch, expected=200):
        bid = batch['batch_id'] if isinstance(batch, dict) else batch
        return self.request(f'{CHANNEL}/batches/{bid}/receipt', None, expected)

    def restart(self):
        """关闭当前实例并以同一个 SQLite 文件起新实例，模拟进程重启。"""
        if self._thread is not None:
            self.server.shutdown()
            self._thread.join()
        if self.persistence is not None:
            close = getattr(self.persistence, 'close', None)
            if callable(close):
                close()
        persistence = RelayPersistence(self.db_path)
        server = RelayServer(('127.0.0.1', 0), persistence, lambda: self.clock[0])
        thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
        thread.start()
        self.server, self.persistence, self._thread = server, persistence, thread
        self.base = f'http://127.0.0.1:{server.server_port}/v1'
        return self

    def close(self):
        try:
            self.server.shutdown()
        except Exception:
            pass
        if self._thread is not None:
            self._thread.join()


def envelope(key='signal-1', lane='coil-north', **extra):
    return dict(producer='signal-source', key=key, lane=lane, payload={'value': 18},
                tags={'feed': 'samples'}) | extra


@contextmanager
def session():
    with TemporaryDirectory(prefix='quaybell-') as directory:
        clock = [1_730_000_000.0]
        db_path = Path(directory) / 'relay.sqlite3'
        persistence = RelayPersistence(db_path)
        server = RelayServer(('127.0.0.1', 0), persistence, lambda: clock[0])
        client = Session(server, clock, db_path=db_path, persistence=persistence)
        thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
        client.attach_thread(thread)
        thread.start()
        try:
            yield client
        finally:
            client.close()
