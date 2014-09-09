import asyncio
from . import Nsqd
from ..selectors import RandomSelector
from ..producer import BaseNsqProducer


class NsqHTTPProducer(BaseNsqProducer):

    def __init__(self, nsqd_http_addresses, selector_factory=RandomSelector,
                 loop=None):
        self._endpoints = nsqd_http_addresses
        self._loop = loop or asyncio.get_event_loop()
        self._selector = selector_factory()
        self._connections = {}

    def _get_connection(self):
        conn_list = list(self._connections.values())
        conn = self._selector.select(conn_list)
        return conn

    def connect(self):
        for host, port in set(self._endpoints):
            conn = Nsqd(host=host, port=port, loop=self._loop)
            self._connections[conn.endpoint] = conn

    @asyncio.coroutine
    def publish(self, topic, message):
        """XXX

        :param topic:
        :param message:
        :return:
        """
        conn = self._get_connection()
        return (yield from conn.pub(topic, message))

    @asyncio.coroutine
    def mpublish(self, topic, message, *messages):
        """XXX

        :param topic:
        :param message:
        :param messages:
        :return:
        """
        conn = self._get_connection()
        return (yield from conn.mpub(topic, message, *messages))

    def close(self):
        for conn in self._connections:
            conn.close()

@asyncio.coroutine
def create_http_producer(nsqd_http_addresses, selector_factory=RandomSelector,
                         loop=None):
    """XXX

    :param nsqd_tcp_addresses:
    :param selector_factory:
    :param loop:
    :return:
    """

    prod = NsqHTTPProducer(nsqd_http_addresses,
                           selector_factory=selector_factory, loop=loop)
    prod.connect()
    return prod
