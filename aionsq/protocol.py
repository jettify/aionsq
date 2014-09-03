"""NSQ protocol parser.

:see: http://nsq.io/clients/tcp_protocol_spec.html
"""
import abc
import struct
import zlib
try:
    import snappy
except ImportError:
    snappy = None

from .exceptions import ProtocolError
from . import consts


__all__ = ['Reader', 'DeflateReader', 'SnappyReader']


_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
    }


class BaseReader:

    def __init__(self):
        self._buffer = bytearray()

    @property
    def buffer(self):
        return self._buffer

    @abc.abstractmethod
    def feed(self, chunk):
        """

        :return:
        """

    @abc.abstractmethod
    def gets(self):
        """

        :return:
        """

    @abc.abstractmethod
    def encode_command(self, cmd, *args, data=None):
        """

        :return:
        """


class BaseCompressReader(BaseReader):

    def compress(self, data):
        raise NotImplementedError()

    def decompress(self, chunk):
        raise NotImplementedError()

    def feed(self, chunk):
        if not chunk:
            return
        uncompressed = self.decompress(chunk)
        uncompressed and self._parser.feed(uncompressed)

    def gets(self):
        return self._parser.gets()

    def encode_command(self, cmd, *args, data=None):
        cmd = self._parser.encode_command(cmd, *args, data=data)
        return self.compress(cmd)


class DeflateReader(BaseCompressReader):

    def __init__(self, buffer='', level=6):
        self._parser = Reader()
        wbits = -zlib.MAX_WBITS
        self._decompressor = zlib.decompressobj(wbits)
        self._compressor = zlib.compressobj(level, zlib.DEFLATED, wbits)
        self.feed(buffer)

    def compress(self, data):
        chunk = self._compressor.compress(data)
        compressed = chunk + self._compressor.flush(zlib.Z_SYNC_FLUSH)
        return compressed

    def decompress(self, chunk):
        return self._decompressor.decompress(chunk)


class SnappyReader(BaseCompressReader):

    def __init__(self, buffer=''):
        self._parser = Reader()
        if not snappy:
            ImportError("You must install snappy first")
        self._decompressor = snappy.StreamDecompressor()
        self._compressor = snappy.StreamCompressor()
        self.feed(buffer)

    def compress(self, data):
        compressed = self._compressor.add_chunk(data, compress=True)
        return compressed

    def decompress(self, chunk):
        return self._decompressor.decompress(chunk)


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


class Reader:

    def __init__(self, buffer=None):

        self._buffer = bytearray()
        self._payload_size = None
        self._is_header = False
        self._frame_type = None
        buffer and self.feed(buffer)


    @property
    def buffer(self):
        return self._buffer

    def feed(self, chunk):
        """Put raw chunk of data obtained from connection to buffer.
        :param data: ``bytes``, raw input data.
        """
        if not chunk:
            return
        self._buffer.extend(chunk)

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
            raise ProtocolError()
        return response_type, response

    def _unpack_error(self):
        start = consts.DATA_SIZE + consts.FRAME_SIZE
        end = consts.DATA_SIZE + self._payload_size
        error = bytes(self._buffer[start:end])
        code, msg = error.split(None, 1)
        return code, msg

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
        timestamp, attempts, msg_id, body = payload
        return timestamp, attempts, msg_id, body

    def encode_command(self, cmd, *args, data=None):
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
