import asyncio
from . import consts
import time
from .log import logger
from .utils import retry_iterator
from .connection import create_connection
from .consts import TOUCH, REQ, FIN, RDY, CLS, MPUB, PUB, SUB, AUTH


@asyncio.coroutine
def create_nsq(host='127.0.0.1', port=4150, loop=None, queue=None,
               heartbeat_interval=30000, feature_negotiation=True,
               tls_v1=False, snappy=False, deflate=False, deflate_level=6,
               sample_rate=0):
    # TODO: add parameters type and value validation
    queue = queue or asyncio.Queue(loop=loop)
    conn = Nsq(host=host, port=port, queue=queue,
               heartbeat_interval=heartbeat_interval,
               feature_negotiation=feature_negotiation,
               tls_v1=tls_v1, snappy=snappy, deflate=deflate,
               deflate_level=deflate_level,
               sample_rate=sample_rate, loop=loop)
    yield from conn.connect()
    return conn



class Nsq:

    def __init__(self, host='127.0.0.1', port=4150, loop=None, queue=None,
               heartbeat_interval=30000, feature_negotiation=True,
               tls_v1=False, snappy=False, deflate=False, deflate_level=6,
               sample_rate=0):
        # TODO: add parameters type and value validation
        self._config = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            'feature_negotiation': feature_negotiation,
        }


        self._host = host
        self._port = port
        self._conn = None
        self._loop = loop
        self._queue = queue or asyncio.Queue(loop=self._loop)

        self._status = consts.INIT
        self._reconnect = True
        self._rdy_state = 0
        self._last_message = None

        self._on_rdy_changed_cb = None
        self._last_rdy = 0


    @asyncio.coroutine
    def connect(self):
        self._conn = yield from create_connection(self._host, self._port,
                                                  self._queue, loop=self._loop)

        self._conn._on_message = self._on_message
        yield from self._conn.identify(**self._config)
        self._status = consts.CONNECTED

    def _on_message(self, msg):
        # should not be coroutine
        # update connections rdy state
        # print('OLD: _rdy_state: {}'.format(self._rdy_state))
        self._rdy_state = self._rdy_state - 1
        # print('NEW: _rdy_state: {}'.format(self._rdy_state))

        self._last_message = time.time()
        if self._on_rdy_changed_cb:
            self._on_rdy_changed_cb(self.id)
        return msg

    @property
    def rdy_state(self):
        return self._rdy_state

    @property
    def in_flight(self):
        return self._conn.in_flight

    @property
    def last_message(self):
        return self._last_message

    @asyncio.coroutine
    def reconnect(self):
        timeout_generator = retry_iterator(init_delay=0.1, max_delay=10.0)
        while not (self._status == consts.CONNECTED):
            try:
                yield from self.connect()
            except ConnectionError:
                logger.error("Can not connect to: {}:{} ".format(
                    self._host, self._port))
            else:
                self._status = consts.CONNECTED
            t = next(timeout_generator)
            yield from asyncio.sleep(t, loop=self._loop)


    @asyncio.coroutine
    def execute(self, command, *args, data=None):
        if self._state <= consts.CONNECTED and self._reconnect:
            yield from self.reconnect()

        response = self._conn.execute(command, *args, data=data)
        return response

    @property
    def id(self):
        return self._conn.endpoint

    def wait_messages(self):
        while True:
            future = asyncio.async(self._queue.get(), loop=self._loop)
            yield future

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
        if not isinstance(count, int):
            raise TypeError('count argument must be int')

        self._last_rdy = count
        self._rdy_state = count
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

    def is_starved(self):
        if self._queue.qsize():
            return False
        starved = (self.in_flight > 0 and
                   self.in_flight >= (self._last_rdy * 0.85))
        return starved

    def __repr__(self):
        return '<Nsq{}>'.format(self._conn.__repr__())
