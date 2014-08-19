import struct
# TODO: improve import
from aionsq.exceptions import make_error
from .constants import NL, HEADER_SIZE, MSG_HEADER, \
    FRAME_TYPE_MESSAGE, FRAME_TYPE_ERROR, FRAME_TYPE_RESPONSE, DATA_SIZE, \
    FRAME_SIZE


class Reader(object):

    def __init__(self):
        self._buffer = bytearray()
        self._payload_size = None
        self._is_header = False
        self._frame_type = None

    def feed(self, data):
        """Put raw chunk of data obtained from connection to buffer.
        :param data: ``bytes``, raw input data.
        """
        if not data:
            return
        self._buffer.extend(data)

    def gets(self):
        buffer_size = len(self._buffer)
        if not self._is_header and buffer_size >= DATA_SIZE:
            size = struct.unpack('>l', self._buffer[:DATA_SIZE])[0]
            self._payload_size = size
            self._is_header = True

        if self._is_header and buffer_size >= DATA_SIZE + self._payload_size:
            start, end = DATA_SIZE, DATA_SIZE + FRAME_SIZE
            self._frame_type = struct.unpack('>l', self._buffer[start:end])[0]

            resp = self._parse_payload()
            self._reset()
            return resp
        return False

    def _reset(self):
        self._buffer = self._buffer[HEADER_SIZE + self._payload_size:]
        self._is_header = False
        self._payload_size = None

    def _parse_payload(self):
        response_type = self._frame_type
        if response_type == FRAME_TYPE_RESPONSE:
            response = self._unpack_response()
        elif response_type == FRAME_TYPE_ERROR:
            response = self._unpack_error()
        elif response_type == FRAME_TYPE_MESSAGE:
            response = self._unpack_message()
        return response_type, response

    def _unpack_error(self):
        start = DATA_SIZE + FRAME_SIZE
        end = DATA_SIZE + self._payload_size
        error = bytes(self._buffer[start:end])
        return make_error(error)

    def _unpack_response(self):
        start = DATA_SIZE + FRAME_SIZE
        end = DATA_SIZE + self._payload_size
        body = bytes(self._buffer[start:end])
        return body

    def _unpack_message(self):
        msg_len = self._payload_size - MSG_HEADER
        fmt = '>qh16s{}s'.format(msg_len)
        payload = struct.unpack(fmt, self._buffer[HEADER_SIZE:])
        timestamp, attempts, msg_id, msg = payload
        return timestamp, attempts, msg_id, msg

def encode_command(cmd, *args, data=None):
    """XXX"""
    assert isinstance(cmd, bytes)
    cmd = cmd.upper().strip()
    body_data, params_data = b'', b''
    if data:
        assert isinstance(data, bytes), 'body must be a bytes'
        body_data = struct.pack('>l', len(data)) + data
    if len(args):
        import ipdb; ipdb.set_trace()
        params = [p.encode('utf-8') if isinstance(p, str) else p for p in args]
        params_data = b' ' + b' '.join(params)
    return b''.join((cmd, params_data, NL, body_data))
