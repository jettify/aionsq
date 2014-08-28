"""NSQ protocol parser.

:see: http://nsq.io/clients/tcp_protocol_spec.html
"""
import struct
from .containers import NsqMessage, NsqErrorMessage
from .exceptions import ProtocolError
from . import consts


__all__ = ['Reader', 'encode_command']


_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
    }


class Reader(object):

    def __init__(self, conn):
        self._buffer = bytearray()
        self._payload_size = None
        self._is_header = False
        self._frame_type = None
        self._conn = conn

    def feed(self, data):
        """Put raw chunk of data obtained from connection to buffer.
        :param data: ``bytes``, raw input data.
        """
        if not data:
            return
        self._buffer.extend(data)

    def gets(self):
        buffer_size = len(self._buffer)
        if not self._is_header and buffer_size >= consts.DATA_SIZE:
            size = struct.unpack('>l', self._buffer[:consts.DATA_SIZE])[0]
            self._payload_size = size
            self._is_header = True

        if (self._is_header and buffer_size >=
                consts.DATA_SIZE + self._payload_size):

            start, end = consts.DATA_SIZE, consts.DATA_SIZE + consts.FRAME_SIZE

            self._frame_type = struct.unpack('>l', self._buffer[start:end])[0]
            resp = self._parse_payload()
            self._reset()
            return resp
        return False

    def _reset(self):
        start = consts.DATA_SIZE + self._payload_size
        self._buffer = self._buffer[start:]
        self._is_header = False
        self._payload_size = None
        self._frame_type = None

    def _parse_payload(self):
        response_type, response = self._frame_type, None
        if response_type == consts.FRAME_TYPE_RESPONSE:
            response = self._unpack_response()
        elif response_type == consts.FRAME_TYPE_ERROR:
            response = self._unpack_error()
        elif response_type == consts.FRAME_TYPE_MESSAGE:
            response = self._unpack_message()
        else:
            raise ProtocolError
        return response_type, response

    def _unpack_error(self):
        start = consts.DATA_SIZE + consts.FRAME_SIZE
        end = consts.DATA_SIZE + self._payload_size
        error = bytes(self._buffer[start:end])
        code, msg = error.split(None, 1)
        return NsqErrorMessage(code=code, msg=msg)

    def _unpack_response(self):
        start = consts.DATA_SIZE + consts.FRAME_SIZE
        end = consts.DATA_SIZE + self._payload_size
        body = bytes(self._buffer[start:end])
        return body

    def _unpack_message(self):
        start = consts.DATA_SIZE + consts.FRAME_SIZE
        end = consts.DATA_SIZE + self._payload_size
        msg_len = end - start - consts.MSG_HEADER
        fmt = '>qh16s{}s'.format(msg_len)
        payload = struct.unpack(fmt, self._buffer[start:end])
        timestamp, attempts, msg_id, msg = payload
        return NsqMessage(timestamp, attempts, msg_id, msg, self._conn)


def _encode_body(data):
    _data = _convert_value(data)
    result = struct.pack('>l', len(_data)) + _data
    return result


def _convert_value(value):
    if type(value) in _converters:
        converted_value = _converters[type(value)](value)
    else:
        raise TypeError("Argument {!r} expected to be of bytes,"
                        " str, int or float type".format(value))
    return converted_value


def encode_command(cmd, *args, data=None):
    """XXX"""
    _cmd = _convert_value(cmd.upper().strip())
    _args = [_convert_value(a) for a in args]
    body_data, params_data = b'', b''

    if len(_args):
        params_data = b' ' + b' '.join(_args)

    if data and isinstance(data, (list, tuple)):
        data_encoded = [_encode_body(part) for part in data]
        num_parts = len(data_encoded)
        payload = struct.pack('>l', num_parts) + b''.join(data_encoded)
        body_data = struct.pack('>l', len(payload)) + payload
    elif data:
        body_data = _encode_body(data)

    return b''.join((_cmd, params_data, consts.NL, body_data))
