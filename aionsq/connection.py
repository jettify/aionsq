import asyncio
import json
import ssl

from collections import deque

from . import consts
from .containers import NsqMessage, NsqErrorMessage
from .exceptions import ProtocolError, make_error
from .protocol import Reader, DeflateReader, SnappyReader


@asyncio.coroutine
def create_connection(host='localhost', port=4151, queue=None, loop=None):
    """XXX"""
    reader, writer = yield from asyncio.open_connection(
            host, port, loop=loop)
    conn = NsqConnection(reader, writer, host, port, queue=queue, loop=loop)
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

        self._parser = Reader()
        # next queue is used for nsq commands
        self._cmd_waiters = deque()
        self._closing = False
        self._closed = False
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)
        # mark connection in upgrading state to ssl socket
        self._is_upgrading = False

    def connect(self):
        self._send_magic()

    def execute(self, command, *args, data=None, cb=None):
        """XXX"""
        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        if command is None:
            raise TypeError("command must not be None")
        if None in set(args):
            raise TypeError("args must not contain None")
        fut = asyncio.Future(loop=self._loop)

        if command in (b'NOP', b'FIN', b'RDY', b'REQ', b'TOUCH'):
            fut.set_result(b'OK')
        else:
            self._cmd_waiters.append((fut, cb))
        command_raw = self._parser.encode_command(command, *args, data=data)
        self._writer.write(command_raw)
        return fut

    @property
    def id(self):
        return "{}:{}".format(self._host, self._port)

    @property
    def closed(self):
        """True if connection is closed."""
        closed = self._closing or self._closed
        if not closed and self._reader and self._reader.at_eof():
            self._closing = closed = True
            self._loop.call_soon(self._do_close, None)
        return closed

    def close(self):
        """Close connection."""
        self._do_close()

    def _do_close(self, exc=None):
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._writer.transport.close()
        self._reader_task.cancel()

    def _send_magic(self):
        self._writer.write(consts.MAGIC_V2)

    def _pulse(self):
        nop = self._parser.encode_command(b'NOP')
        self._writer.write(nop)

    @asyncio.coroutine
    def _upgrade_to_tls(self):
        self._reader_task.cancel()
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)

        self._reader, self._writer = yield from asyncio.open_connection(
            sock=raw_sock, ssl=ssl_context, loop=self._loop,
            server_hostname=self._host)
        bin_ok = yield from self._reader.readexactly(10)
        if bin_ok != consts.BIN_OK:
            raise RuntimeError('Upgrade to TLS failed, got: {}'.format(bin_ok))
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)


    def _upgrade_to_snappy(self):
        self._parser = SnappyReader(self._parser.buffer)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))
        return fut

    def _upgrade_to_deflate(self):
        self._parser = DeflateReader(self._parser.buffer)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))
        return fut

    @asyncio.coroutine
    def _read_data(self, num_obj=1):
        """Response reader task."""
        is_canceled = False
        while not self._reader.at_eof():
            try:
                data = yield from self._reader.read(consts.MAX_CHUNK_SIZE)
            except asyncio.CancelledError:
                is_canceled = True
                break
            except Exception as exc:
                break
            self._parser.feed(data)
            not self._is_upgrading and self._read_buffer()

        if is_canceled:
            # useful during update to TLS, task canceled but connection
            # should not be closed
            return
        self._closing = True
        self._loop.call_soon(self._do_close, None)

    def _parse_data(self):
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
                return False

            resp_type, resp = obj
            if resp_type == consts.FRAME_TYPE_RESPONSE and \
                            resp == consts.HEARTBEAT:
                self._pulse()
            elif resp_type == consts.FRAME_TYPE_RESPONSE:
                waiter, cb = self._cmd_waiters.popleft()
                waiter.set_result(resp)
                cb is not None and cb(resp)
            elif resp_type == consts.FRAME_TYPE_ERROR:
                waiter, cb = self._cmd_waiters.popleft()
                error = make_error(*resp)
                waiter.set_exception(error)
                cb is not None and cb(resp)
            elif resp_type == consts.FRAME_TYPE_MESSAGE:
                ts, att, msg_id, body = resp
                msg = NsqMessage(ts, att, msg_id, body, self)
                self._msq_queue.put_nowait(msg)
            return True

    def _read_buffer(self):
        is_continue = True
        while is_continue:
            is_continue = self._parse_data()

    def _start_upgrading(self, resp=None):
        self._is_upgrading = True

    def _finish_upgrading(self, resp=None):
        self._read_buffer()
        self._is_upgrading = False

    @asyncio.coroutine
    def identify(self, **config):
        # TODO: add config validator
        data = json.dumps(config)
        resp = yield from self.execute(
            b'IDENTIFY', data=data, cb=self._start_upgrading)
        if resp in (b'OK', 'OK'):
            self._finish_upgrading()
            return resp
        resp_config = json.loads(resp.decode('utf-8'))
        fut = None
        if resp_config.get('tls_v1'):
            yield from self._upgrade_to_tls()

        if resp_config.get('snappy'):
            fut = self._upgrade_to_snappy()
        elif resp_config.get('deflate'):
            fut = self._upgrade_to_deflate()
        self._finish_upgrading()
        if fut:
            ok = yield from fut
            assert ok == b'OK'
        return resp

    def __repr__(self):
        return '<NsqConnection: {}:{}'.format(self._host, self._port)
