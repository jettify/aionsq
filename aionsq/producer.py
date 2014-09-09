import abc
import asyncio

from .nsq import create_nsq
from .selectors import RandomSelector


class BaseNsqProducer(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def publish(self, topic, message):
        """XXX

        :param topic:
        :param message:
        :return:
        """
        pass

    @abc.abstractmethod
    def mpublish(self, topic, message, *messages):
        """

        :param topic:
        :param message:
        :param messages:
        :return:
        """
        pass

    @abc.abstractmethod
    def close(self):
        pass


class NsqTCPProducer(BaseNsqProducer):

    def __init__(self, nsqd_tcp_addresses, conn_config,
                 selector_factory=RandomSelector, loop=None):
        self._endpoints = nsqd_tcp_addresses
        self._loop = loop or asyncio.get_event_loop()
        self._conn_config = conn_config
        self._selector = selector_factory()
        self._connections = {}

    def _get_connection(self):
        conn_list = list(self._connections.values())
        conn = self._selector.select(conn_list)
        return conn

    def connect(self):
        for host, port in self._endpoints:
            conn = yield from create_nsq(host=host, port=port, loop=self._loop,
                                         **self._conn_config)
            self._connections[conn.id] = conn

    def publish(self, topic, message):
        """XXX

        :param topic:
        :param message:
        :return:
        """
        conn = self._get_connection()
        return (yield from conn.pub(topic, message))

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
def create_producer(nsqd_tcp_addresses, conn_config,
                    selector_factory=RandomSelector, loop=None):
    """XXX

    :param nsqd_tcp_addresses:
    :param conn_config:
    :param selector_factory:
    :param loop:
    :return:
    """

    prod = NsqTCPProducer(nsqd_tcp_addresses, conn_config,
                          selector_factory=selector_factory,
                          loop=loop)
    yield from prod.connect()
    return prod


