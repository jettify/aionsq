import asyncio
from collections import deque


class BasicNsqClient(object):
    """Experiment purposes"""

    def __init__(self, nsqd_tcp_addresses=[], max_in_flight=2500, loop=None):
        self._nsqd_tcp_addresses = nsqd_tcp_addresses

        self.max_in_flight = max_in_flight
        self._message_queue = asyncio.Queue()
        self._loop = loop or asyncio.get_event_loop()
        self._connections = deque()

        self._lookupd = None
        self._producers = []
        self._lookup_sleep_time = 30

    @asyncio.coroutine
    def connect(self):
        pass

    @asyncio.coroutine
    def _distribute_rdy(self):
        pass

    @asyncio.coroutine
    def subscribe(self, topic, channel):
        pass

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
        # import ipdb; ipdb.set_trace()
        resp = yield from conn.pub(topic, message)
        self._return_connection(conn)
        return resp

    def _query_lookupd(self):
        while True:
            producers = yield from self._lookupd.lookup()
            asyncio.sleep(self._lookup_sleep_time, loop=self._loop)
            print(producers)
