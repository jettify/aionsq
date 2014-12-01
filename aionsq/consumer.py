import asyncio
import random
from collections import deque
import time
from aionsq.http import NsqLookupd
from aionsq.nsq import create_nsq
from aionsq.utils import RdyControl


class NsqConsumer:
    """Experiment purposes"""

    def __init__(self, nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                 max_in_flight=42, loop=None):

        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []

        self._max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)

        self._connections = {}

        self._idle_timeout = 10

        self._rdy_control = None
        self._max_in_flight = max_in_flight

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec

    @asyncio.coroutine
    def connect(self):
        if self._lookupd_http_addresses:
            self._lookupd_task = asyncio.Task(self._lookupd(), loop=self._loop)

        if self._nsqd_tcp_addresses:
            for host, port in self._nsqd_tcp_addresses:
                conn = yield from create_nsq(host, port, queue=self._queue,
                                             loop=self._loop)
            self._connections[conn.id] = conn


        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight,
                                       loop=self._loop)
        self._rdy_control.add_connections(self._connections)

    @asyncio.coroutine
    def _poll_lookupd(self, host, port):
        conn = NsqLookupd(host, port, loop=self.loop)
        res = yield from conn.lookup('foo')

        for producer in res['producers']:
            host = producer['broadcast_address']
            port = producer['tcp_port']
            conn = yield from create_nsq(host, port, queue=self._queue,
                                         loop=self._loop)
            self._connections[conn.id] = conn
        conn.close()

    @asyncio.coroutine
    def subscribe(self, topic, channel):
        self._is_subscribe = True
        for conn in self._connections.values():
            yield from conn.sub(topic, channel)
        self._redistribute_task = asyncio.Task(self._redistribute(),
                                               loop=self._loop)

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            fut = asyncio.async(self._queue.get(), loop=self._loop)
            yield fut

    def is_starved(self):
        conns = self._connections.values()
        return any(conn.is_starved() for conn in conns)

    @asyncio.coroutine
    def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            yield from asyncio.sleep(self._redistribute_timeout,
                                     loop=self._loop)

    @asyncio.coroutine
    def _lookupd(self):
        while self._is_subscribe:
            yield from asyncio.sleep(self._redistribute_timeout,
                                     loop=self._loop)
            host, port = random.choice(self._lookupd_http_addresses)
            yield from self._poll_lookupd(host, port)

