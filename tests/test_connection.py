from ._testutils import run_until_complete, BaseTest
from aionsq.connection import create_connection, NsqConnection
from aionsq.protocol import Reader, SnappyReader, DeflateReader


class NsqConnectionTest(BaseTest):

    @run_until_complete
    def test_basic_instance(self):
        host, port = '127.0.0.1', 4150
        nsq = yield from create_connection(host=host, port=port,
                                           loop=self.loop)
        self.assertIsInstance(nsq, NsqConnection)
        self.assertTrue('NsqConnection' in nsq.__repr__())
        self.assertTrue(not nsq.closed)
        self.assertTrue(host in nsq.endpoint)
        self.assertTrue(str(port) in nsq.endpoint)
        nsq.close()
        self.assertEqual(nsq.closed, True)

    @run_until_complete
    def test_pub_sub(self):
        host, port = '127.0.0.1', 4150
        conn = yield from create_connection(host=host, port=port,
                                           loop=self.loop)
        yield from self._pub_sub_rdy_fin(conn)

    @run_until_complete
    def test_tls(self):
        host, port = '127.0.0.1', 4150
        conn = yield from create_connection(host=host, port=port,
                                           loop=self.loop)

        config = {'feature_negotiation':True, 'tls_v1': True,
                  'snappy': False, 'deflate': False
        }

        yield from conn.identify(**config)
        yield from self._pub_sub_rdy_fin(conn)

    @run_until_complete
    def test_snappy(self):
        host, port = '127.0.0.1', 4150
        conn = yield from create_connection(host=host, port=port,
                                           loop=self.loop)
        config = {'feature_negotiation':True, 'tls_v1': False,
                  'snappy': True, 'deflate': False
        }
        self.assertIsInstance(conn._parser, Reader)
        yield from conn.identify(**config)
        self.assertIsInstance(conn._parser, SnappyReader)

        yield from self._pub_sub_rdy_fin(conn)

    @run_until_complete
    def test_deflate(self):
        host, port = '127.0.0.1', 4150
        conn = yield from create_connection(host=host, port=port,
                                           loop=self.loop)

        config = {'feature_negotiation':True, 'tls_v1': False,
                  'snappy': False, 'deflate': True
        }
        self.assertIsInstance(conn._parser, Reader)

        yield from conn.identify(**config)
        self.assertIsInstance(conn._parser, DeflateReader)
        yield from self._pub_sub_rdy_fin(conn)

    def _pub_sub_rdy_fin(self, conn):
        ok = yield from conn.execute(b'PUB', b'foo', data=b'msg foo')
        self.assertEqual(ok, b'OK')
        yield from conn.execute(b'SUB', b'foo', b'bar')
        yield from conn.execute(b'RDY', 1)
        msg = yield from conn._queue.get()
        self.assertEqual(msg.processed, False)
        yield from msg.fin()
        self.assertEqual(msg.processed, True)
        yield from conn.execute(b'CLS')
