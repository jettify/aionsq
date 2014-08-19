import unittest
from aionsq.protocol import Reader, encode_command


class ParserTest(unittest.TestCase):

    def test_ok_resp(self):
        ok_raw = b'\x00\x00\x00\x06\x00\x00\x00\x00OK'
        parser = Reader()
        parser.feed(ok_raw)
        obj_type, obj = parser.gets()
        self.assertEqual(b'OK', obj)
        self.assertEqual(0, obj_type)

    def test_heartbeat_resp(self):
        heartbeat_msg = b'\x00\x00\x00\x0f\x00\x00\x00\x00_heartbeat_'
        parser = Reader()
        parser.feed(heartbeat_msg)
        obj_type, obj = parser.gets()
        self.assertEqual(b'_heartbeat_', obj)
        self.assertEqual(0, obj_type)

