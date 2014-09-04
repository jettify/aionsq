import asyncio
from .connection import create_connection


@asyncio.coroutine
def create_nsq(host='127.0.0.1', port=4151, queue=None,
               heartbeat_interval=30000, feature_negotiation=True, tls_v1=True,
               snappy=False, deflate=False, deflate_level=6,
               output_buffer_timeout='250ms',
               output_buffer_size=16,
               sample_rate=0, loop=None):
    # TODO: add parameters type and value validation
    config = {
        "deflate": deflate,
        "deflate_level": deflate_level,
        "sample_rate": sample_rate,
        "snappy": snappy,
        "tls_v1": tls_v1,
        "heartbeat_interval": heartbeat_interval,
        'feature_negotiation': feature_negotiation,
        # 'output_buffer_timeout': output_buffer_timeout,
        # 'output_buffer_size': output_buffer_size,
    }
    queue = queue or asyncio.Queue(loop)
    conn = yield from create_connection(host=host, port=port, queue=queue, loop=loop)
    yield from conn.identify(**config)
    return Nsq(conn, queue, loop=loop)


class Nsq:

    def __init__(self, conn, queue=None, loop=None):
        self._conn = conn
        self._loop = loop
        self._queue = queue or asyncio.Queue(self._loop)


    @asyncio.coroutine
    def wait_messages(self):
        return (yield from self._queue.get())

    @asyncio.coroutine
    def auth(self, secret):
        """

        :param secret:
        :return:
        """
        return (yield from self._conn.execute(b'AUTH', data=secret))

    @asyncio.coroutine
    def sub(self, topic, channel):
        """

        :param topic:
        :param channel:
        :return:
        """
        return (yield from self._conn.execute(b'SUB', topic, channel))

    @asyncio.coroutine
    def pub(self, topic, message):
        """

        :param topic:
        :param message:
        :return:
        """
        return (yield from self._conn.execute(b'PUB', topic, data=message))

    @asyncio.coroutine
    def mpub(self, topic, message, *messages):
        """

        :param topic:
        :param message:
        :param messages:
        :return:
        """
        msgs = [message] + messages
        return (yield from self._conn.execute(b'MPUB', topic, data=msgs))

    @asyncio.coroutine
    def rdy(self, count):
        """

        :param count:
        :return:
        """
        return (yield from self._conn.execute(b'RDY', count))

    @asyncio.coroutine
    def fin(self, message_id):
        """

        :param message_id:
        :return:
        """
        return (yield from self._conn.execute(b'FIN', message_id))

    @asyncio.coroutine
    def req(self, message_id, timeout):
        """

        :param message_id:
        :param timeout:
        :return:
        """
        return (yield from self._conn.execute(b'REQ', message_id, timeout))

    @asyncio.coroutine
    def touch(self, message_id):
        """

        :param message_id:
        :return:
        """
        return (yield from self._conn.execute(b'TOUCH', message_id))

    @asyncio.coroutine
    def cls(self):
        """

        :return:
        """
        yield from self._conn.execute(b'CLS')
        self.close()

    def close(self):
        self._conn.close()

    def __repr__(self):
        return '<Nsq{}>'.format(self._conn.__repr__())
