import asyncio
from collections import deque
import time
from aionsq.nsq import create_nsq
from aionsq.utils import RdyControl


class NsqConsumer:
    """Experiment purposes"""

    def __init__(self, nsqd_tcp_addresses=None, max_in_flight=42, loop=None):

        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []

        self.max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)

        self._connections = {}

        self._idle_timeout = 10

        self._rdy_control = None
        self._max_in_flight = max_in_flight

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec

    @asyncio.coroutine
    def connect(self):
        for host, port in self._nsqd_tcp_addresses:
            conn = yield from create_nsq(host, port, queue=self._queue,
                                         loop=self._loop)
            self._connections[conn.id] = conn

        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight,
                                       loop=self._loop)
        self._rdy_control.add_connections(self._connections)

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

    # @asyncio.coroutine
    # def _lookupd_task(self):
    #     while self._is_subscribe:
    #         # TODO: poll lookupd
    #         yield from asyncio.sleep(self._redistribute_timeout,
    #                                  loop=self._loop)
