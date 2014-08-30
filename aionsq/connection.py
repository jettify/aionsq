import asyncio
from asyncio import CancelledError
from collections import deque
from .consts import MAX_CHUNK_SIZE, MAGIC_V2, FRAME_TYPE_RESPONSE, \
    FRAME_TYPE_ERROR, FRAME_TYPE_MESSAGE, HEARTBEAT
from .exceptions import ProtocolError
from .protocol import Reader, DeflateReader, SnappyReader

import ssl

@asyncio.coroutine
def create_connection(host='localhost', port=4151, *, loop=None):
    """XXX"""
    reader, writer = yield from asyncio.open_connection(
            host, port, loop=loop)
    conn = NsqConnection(reader, writer, host, port, loop=loop)
    conn.connect()
    return conn


class NsqConnection:
    """XXX"""

    def __init__(self, reader, writer, host, port, *, queue=None, loop=None):

        self._reader, self._writer = reader, writer
        self._host, self._port = host, port

        self._loop = loop or asyncio.get_event_loop()

        assert isinstance(queue, asyncio.Queue) or queue is None
        self._msq_queue = queue or asyncio.Queue(loop=self._loop)

        self._parser = Reader(self)
        # next queue is used for nsq commands
        self._cmd_waiters = deque()
        self._closing = False
        self._closed = False
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)
        # mart connection in upgrading state to ssl socket
        self._is_upgrading = False

    @asyncio.coroutine
    def upgrade_to_tls(self):
        self._reader_task.cancel()

        transport = self._writer.transport
        transport.pause_reading()
        rawsock = transport.get_extra_info('socket', default=None)
        if rawsock is None:
            raise RuntimeError("Transport does not expose socket instance")

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)

        reader, writer = yield from asyncio.open_connection(
            sock=rawsock, ssl=ssl_context, loop=self._loop,
            server_hostname=self._host)

        self._reader = reader
        self._writer = writer

        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)

    def upgrade_to_snappy(self):
        self._parser = SnappyReader(self._parser)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))

    def upgrade_to_deflate(self):
        self._parser = DeflateReader(self._parser)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))


    def auth(self, secret):
        pass

# b'\x00\n\x00\xf5\xff\x00\x00\x00\x06\x00\x00\x00\x00OK\x00\x00\x00\xff\xff\x00\x00\x00\xff\xff'

    @property
    def id(self):
        return "nsq {}:{}".format(self._host, self._port)

    def __repr__(self):
        return '<NsqConnection: {}:{}'.format(self._host, self._port)

    def connect(self):
        self._send_magic()

    def execute(self, command, *args, data=None):
        """XXX"""
        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        if command is None:
            raise TypeError("command must not be None")
        if None in set(args):
            raise TypeError("args must not contain None")
        cb = None
        fut = asyncio.Future(loop=self._loop)

        if command in (b'NOP', b'FIN', b'RDY', b'REQ', b'TOUCH'):
            fut.set_result(b'OK')
        else:
            self._cmd_waiters.append((fut, cb))
        command_raw = self._parser.encode_command(command, *args, data=data)
        self._writer.write(command_raw)
        return fut

    def close(self):
        """Close connection."""
        cls = self._parser.encode_command(b'CLS')
        return self.execute(cls)


    def _do_close(self, exc):
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._writer.transport.close()

    @property
    def closed(self):
        """True if connection is closed."""
        closed = self._closing or self._closed
        if not closed and self._reader and self._reader.at_eof():
            self._closing = closed = True
            self._loop.call_soon(self._do_close, None)
        return closed

    def _send_magic(self):
        self._writer.write(MAGIC_V2)

    def _pulse(self):
        nop = self._parser.encode_command(b'NOP')
        self._writer.write(nop)

    @asyncio.coroutine
    def _read_data(self, forever=True):
        """Response reader task."""
        is_canceled = False
        while not self._reader.at_eof():
            try:
                data = yield from self._reader.read(MAX_CHUNK_SIZE)
                # import ipdb; ipdb.set_trace()
            except CancelledError:

                is_canceled = True
                break
            except Exception as exc:
                import ipdb; ipdb.set_trace()
                break
            self._parser.feed(data)
            while True:
                try:
                    obj = self._parser.gets()
                except ProtocolError as exc:
                    # ProtocolError is fatal
                    # so connection must be closed
                    self._closing = True
                    self._loop.call_soon(self._do_close, exc)
                    return
                else:
                    if obj is False:
                        break

                    resp_type, resp = obj
                    if resp_type == FRAME_TYPE_RESPONSE and resp == HEARTBEAT:
                        self._pulse()
                    elif resp_type == FRAME_TYPE_RESPONSE:
                        waiter, cb = self._cmd_waiters.popleft()
                        import ipdb; ipdb.set_trace()
                        waiter.set_result(resp)
                        cb is not None and cb(resp)
                    elif resp_type == FRAME_TYPE_ERROR:
                        waiter, cb = self._cmd_waiters.popleft()
                        waiter.set_exception(resp)
                        cb is not None and cb(resp)
                    elif resp_type == FRAME_TYPE_MESSAGE:
                        self._msq_queue.put_nowait(resp)

        if not is_canceled:
            self._closing = True
            self._loop.call_soon(self._do_close, None)
        import ipdb; ipdb.set_trace()
        pass

class Nsq(object):

    def __init__(self, connection):
        self._conn = connection


    def __repr__(self):
        return '<Nsq {!r}>'.format(self._conn)

    @asyncio.coroutine
    def nop(self):
        """

        :return:
        """
        return (yield from self._conn.execute(b'NOP'))

    @asyncio.coroutine
    def auth(self, secret):
        """

        :param secret:
        :return:
        """
        return (yield from self._conn.execute(b'AUTH', data=secret))

    @asyncio.coroutine
    def sub(self, topic, channel):
        return (yield from self._conn.execute(b'SUB', topic, channel))

    def pub(self, topic, message):
        """

        :param topic:
        :param message:
        :return:
        """
        return (yield from self._conn.execute(b'PUB', topic, data=message))

    def mpub(self, topic, message, *messages):
        """

        :param topic:
        :param message:
        :param messages:
        :return:
        """
        msgs = [message] + messages
        return (yield from self._conn.execute(b'MPUB', topic, data=msgs))

    def rdy(self, count):
        """

        :param count:
        :return:
        """
        return (yield from self._conn.execute(b'RDY', count))


    def fin(self, message_id):
        """

        :param message_id:
        :return:
        """
        return (yield from self._conn.execute(b'FIN', message_id))

    def req(self, message_id, timeout):
        """

        :param message_id:
        :param timeout:
        :return:
        """
        return (yield from self._conn.execute(b'REQ', message_id, timeout))

    def touch(self, message_id):
        """

        :param message_id:
        :return:
        """
        return (yield from self._conn.execute(b'TOUCH', message_id))

    def cls(self):
        """

        :return:
        """
        return (yield from self._conn.execute(b'CLS'))
