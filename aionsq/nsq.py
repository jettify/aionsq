import asyncio

from .connection import create_connection
from .consts import TOUCH, REQ, FIN, RDY, CLS, MPUB, PUB, SUB, AUTH


@asyncio.coroutine
def create_nsq(host='127.0.0.1', port=4150, loop=None, queue=None,
               heartbeat_interval=30000, feature_negotiation=True,
               tls_v1=False, snappy=False, deflate=False, deflate_level=6,
               sample_rate=0):
    # TODO: add parameters type and value validation
    config = {
        "deflate": deflate,
        "deflate_level": deflate_level,
        "sample_rate": sample_rate,
        "snappy": snappy,
        "tls_v1": tls_v1,
        "heartbeat_interval": heartbeat_interval,
        'feature_negotiation': feature_negotiation,
    }
    queue = queue or asyncio.Queue(loop)
    conn = yield from create_connection(host=host, port=port, queue=queue,
                                        loop=loop)
    yield from conn.identify(**config)
    return Nsq(conn, loop=loop)


class Nsq:

    def __init__(self, conn, loop=None):
        self._conn = conn
        self._loop = loop
        self._queue = self._conn.queue

    @property
    def id(self):
        return self._conn.endpoint

    @asyncio.coroutine
    def wait_messages(self):
        return (yield from self._queue.get())

    @asyncio.coroutine
    def auth(self, secret):
        """

        :param secret:
        :return:
        """
        return (yield from self._conn.execute(AUTH, data=secret))

    @asyncio.coroutine
    def sub(self, topic, channel):
        """

        :param topic:
        :param channel:
        :return:
        """
        return (yield from self._conn.execute(SUB, topic, channel))

    @asyncio.coroutine
    def pub(self, topic, message):
        """

        :param topic:
        :param message:
        :return:
        """
        return (yield from self._conn.execute(PUB, topic, data=message))

    @asyncio.coroutine
    def mpub(self, topic, message, *messages):
        """

        :param topic:
        :param message:
        :param messages:
        :return:
        """
        msgs = [message] + list(messages)
        return (yield from self._conn.execute(MPUB, topic, data=msgs))

    @asyncio.coroutine
    def rdy(self, count):
        """

        :param count:
        :return:
        """
        return (yield from self._conn.execute(RDY, count))

    @asyncio.coroutine
    def fin(self, message_id):
        """

        :param message_id:
        :return:
        """
        return (yield from self._conn.execute(FIN, message_id))

    @asyncio.coroutine
    def req(self, message_id, timeout):
        """

        :param message_id:
        :param timeout:
        :return:
        """
        return (yield from self._conn.execute(REQ, message_id, timeout))

    @asyncio.coroutine
    def touch(self, message_id):
        """

        :param message_id:
        :return:
        """
        return (yield from self._conn.execute(TOUCH, message_id))

    @asyncio.coroutine
    def cls(self):
        """

        :return:
        """
        yield from self._conn.execute(CLS)
        self.close()

    def close(self):
        self._conn.close()

    def __repr__(self):
        return '<Nsq{}>'.format(self._conn.__repr__())
