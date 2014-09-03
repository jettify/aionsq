import asyncio
from collections import deque
from .connection import create_connection


class BasicNsqClient(object):
    """Experiment purposes"""

    def __init__(self, nsqd_tcp_addresses=[], max_in_flight=2500, loop=None):
        self._nsqd_tcp_addresses = nsqd_tcp_addresses

        self.max_in_flight = max_in_flight
        self._message_queue = asyncio.Queue()
        self._loop = loop or asyncio.get_event_loop()
        self._connections = deque()

    @asyncio.coroutine
    def connect(self):
        for host, port in self._nsqd_tcp_addresses:
            conn = yield from create_connection(
                host, port, queue=self._message_queue, loop=self._loop)
            import ipdb; ipdb.set_trace()
            self._connections.append(conn)

    def _get_connection(self):
        return self._connections.popleft()

    def _return_connection(self, conn):
        self._connections.append(conn)

    @asyncio.coroutine
    def _distribute_rdy(self):
        conn = self._get_connection()
        yield from conn.rdy(1)
        self._return_connection(conn)

    @asyncio.coroutine
    def subscribe(self, topic, channel):
        self._is_subsribe = True
        conn = self._get_connection()
        yield from conn.sub(topic, channel)
        self._return_connection(conn)

    def wait_messages(self):
        if not self._is_subsribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subsribe:
            fut = asyncio.async(self._message_queue.get(), loop=self._loop)
            yield from self._distribute_rdy()
            yield fut

    @asyncio.coroutine
    def publish(self, topic, message):
        conn = self._get_connection()
        import ipdb; ipdb.set_trace()
        resp = yield from conn.pub(topic, message)
        self._return_connection(conn)
        return resp
