import unittest
from aionsq.exceptions import ProtocolError
from aionsq.protocol import Reader, encode_command
from aionsq.containers import NsqMessage, NsqErrorMessage


class ParserTest(unittest.TestCase):
    
    def setUp(self):
        self.conn = object()
        self.parser = Reader(self.conn)

    def test_ok_resp(self):
        ok_raw = b'\x00\x00\x00\x06\x00\x00\x00\x00OK'
        self.parser.feed(ok_raw)
        obj_type, obj = self.parser.gets()
        self.assertEqual(b'OK', obj)
        self.assertEqual(0, obj_type)

    def test_heartbeat_resp(self):
        heartbeat_msg = b'\x00\x00\x00\x0f\x00\x00\x00\x00_heartbeat_'
        self.parser.feed(heartbeat_msg)
        obj_type, obj = self.parser.gets()
        self.assertEqual(b'_heartbeat_', obj)
        self.assertEqual(0, obj_type)

    def test_msg_resp(self):
        msg = b'\x00\x00\x00&\x00\x00\x00\x02\x13\x8c4\xcd\x01x~\x83' \
              b'\x00\x0106f6cbf50539f004test_msg\x00\x00\x00\x0f\x00' \
              b'\x00\x00\x00_heartbeat_'
        self.parser.feed(msg)

        # unpack message
        obj_type, obj = self.parser.gets()
        self.assertEqual(2, obj_type)
        msg_tuple = (1408558838557736579, 1, b'06f6cbf50539f004',
                     b'test_msg', self.conn)
        self.assertEqual(obj, msg_tuple)
        self.assertIsInstance(obj, NsqMessage)

        # unpack heartbeat
        obj_type, obj = self.parser.gets()
        self.assertEqual(0, obj_type)
        self.assertEqual(b'_heartbeat_', obj)

    def test_chunked_read(self):
        msg = b'\x00\x00\x00&\x00\x00\x00\x02\x13\x8c4\xcd\x01x~\x83' \
              b'\x00\x0106f6cbf50539f004test_msg\x00\x00\x00\x0f\x00' \
              b'\x00\x00\x00_heartbeat_'

        responses = []

        # empty data
        self.parser.feed(bytes(b''))
        # reads one character in time
        for i in range(len(msg)):
            char = msg[i:i+1]
            self.parser.feed(bytes(char))
            resp = self.parser.gets()
            if resp is not False:
                responses.append(resp)

        self.assertEqual(len(responses), 2)
        # unpack msg
        obj_type, obj = responses[0]
        self.assertEqual(2, obj_type)
        msg_tuple = (1408558838557736579, 1, b'06f6cbf50539f004',
                     b'test_msg', self.conn)
        self.assertEqual(obj, msg_tuple)

        # unpack heartbeat
        obj_type, obj = responses[1]
        self.assertEqual(0, obj_type)
        self.assertEqual(b'_heartbeat_', obj)

    def test_error_resp(self):
        error_msg = b'\x00\x00\x002\x00\x00\x00\x01E_BAD_TOPIC PUB topic ' \
                    b'name "fo/o" is not valid'
        self.parser.feed(error_msg)
        obj_type, obj = self.parser.gets()
        self.assertEqual(1, obj_type)
        code, msg = obj
        self.assertEqual(b'E_BAD_TOPIC', code)
        self.assertEqual(b'PUB topic name "fo/o" is not valid', msg)
        self.assertIsInstance(obj, NsqErrorMessage)

    def test_protocol_error(self):
        ok_raw = b'\x00\x00\x00\x06\x00\x00\x00\x03OK'
        self.parser.feed(ok_raw)
        with self.assertRaises(ProtocolError):
            self.parser.gets()


class CommandEncoderTest(unittest.TestCase):

    def test_sub_command(self):
        command_raw = encode_command(b'SUB', b'foo', b'bar')
        command_str = encode_command('SUB', 'foo', 'bar')
        self.assertEqual(command_raw, b'SUB foo bar\n')
        self.assertEqual(command_str, b'SUB foo bar\n')

    def test_pub_command(self):
        command_raw = encode_command(b'PUB', b'foo', data=b'test_msg')
        command_str = encode_command('PUB', 'foo', data='test_msg')
        self.assertEqual(command_raw, b'PUB foo\n\x00\x00\x00\x08test_msg')
        self.assertEqual(command_str, b'PUB foo\n\x00\x00\x00\x08test_msg')

    def test_pub_different_payload(self):
        cmd_with_int = encode_command('PUB', b'foo', data=42)
        cmd_with_float = encode_command('PUB', 'foo', data=3.14)
        cmd_with_bytearray = encode_command('PUB', 'foo',
                                            data=bytearray(b'foo'))

        self.assertEqual(cmd_with_int, b'PUB foo\n\x00\x00\x00\x0242')
        self.assertEqual(cmd_with_float, b'PUB foo\n\x00\x00\x00\x043.14')
        self.assertEqual(cmd_with_bytearray, b'PUB foo\n\x00\x00\x00\x03foo')

    def test_pub_not_converatble_payload(self):
        with self.assertRaises(TypeError):
            encode_command(b'PUB', b'foo', data=object())

    def test_nop_command(self):
        command_raw = encode_command(b'NOP')
        self.assertEqual(command_raw, b'NOP\n')

    def test_mpub_command(self):
        command_raw = encode_command(b'MPUB', b'topic', data=[b'foo', b'bar'])
        required_command = b'MPUB topic\n\x00\x00\x00\x12\x00\x00\x00\x02' \
                           b'\x00\x00\x00\x03foo\x00\x00\x00\x03bar'
        self.assertEqual(command_raw, required_command)

        command_raw = encode_command(b'MPUB', 'topic', data=['foo', 'bar'])
        self.assertEqual(command_raw, required_command)
