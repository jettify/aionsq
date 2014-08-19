import asyncio
from collections import deque
from aionsq.constants import MAX_CHUNK_SIZE, MAGIC_V2, FRAME_TYPE_RESPONSE, \
    FRAME_TYPE_ERROR, FRAME_TYPE_MESSAGE
from aionsq.exceptions import ProtocolError
from .protocol import encode_command, Reader


@asyncio.coroutine
def create_connection(host='localhost', port=4151, *, loop=None):
    """XXX"""
    reader, writer = yield from asyncio.open_connection(
            host, port, loop=loop)

    conn = NsqConnection(reader, writer, loop=loop)

    # if password is not None:
    #     yield from conn.auth(password)
    # if db is not None:
    #     yield from conn.select(db)
    conn.connect()
    return conn


class NsqConnection:
    """Redis connection."""

    def __init__(self, reader, writer, *, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._reader = reader
        self._writer = writer
        self._loop = loop or asyncio.get_event_loop()
        self._parser = Reader()
        self._cmd_waiters = deque()
        self._msq_queue = asyncio.Queue(loop=self._loop)
        self._closing = False
        self._closed = False

        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)


    def __repr__(self):
        return '<NsqProducer [db:{}]>'.format(self._db)

    def connect(self):
        self._writer.write(MAGIC_V2)

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
            return fut
        self._cmd_waiters.append((fut, cb))
        self._writer.write(encode_command(command, *args, data=data))
        return fut

    def close(self):
        """Close connection."""
        self._do_close(None)

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

    @asyncio.coroutine
    def _read_data(self):
        """Response reader task."""
        while not self._reader.at_eof():
            try:
                data = yield from self._reader.read(MAX_CHUNK_SIZE)
                import ipdb; ipdb.set_trace()
            except Exception as exc:
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
                    if resp_type == FRAME_TYPE_RESPONSE:
                        waiter, cb = self._cmd_waiters.popleft()
                        self._cmd_waiters.set_result(resp)
                        cb is not None and cb(resp)
                    elif resp_type == FRAME_TYPE_ERROR:
                        self._cmd_waiters.set_exception(resp)
                        cb is not None and cb(resp)
                    elif resp_type == FRAME_TYPE_MESSAGE:
                        self._msq_queue.put_nowait(resp)

        self._closing = True
        self._loop.call_soon(self._do_close, None)