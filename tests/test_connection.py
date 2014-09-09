import asyncio
from ._testutils import run_until_complete, BaseTest
from aionsq.connection import create_connection, NsqConnection
from aionsq.http import Nsqd
from aionsq.protocol import Reader, SnappyReader, DeflateReader


class NsqConnectionTest(BaseTest):

    def setUp(self):
        self.topic = b'foo'
        self.host = '127.0.0.1'
        self.port = 4150
        super().setUp()

    def tearDown(self):
        conn = Nsqd(self.host, self.port+1, loop=self.loop)
        try:
            self.loop.run_until_complete(conn.delete_topic(self.topic))
        except Exception:
            # TODO: fix
            pass
        super().tearDown()

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
        conn = yield from create_connection(host=self.host, port=self.port,
                                            loop=self.loop)

        yield from self._pub_sub_rdy_fin(conn)

    @run_until_complete
    def test_tls(self):
        conn = yield from create_connection(host=self.host, port=self.port,
                                            loop=self.loop)

        config = {'feature_negotiation':True, 'tls_v1': True,
                  'snappy': False, 'deflate': False
        }

        yield from conn.identify(**config)
        yield from self._pub_sub_rdy_fin(conn)

    @run_until_complete
    def test_snappy(self):
        conn = yield from create_connection(host=self.host, port=self.port,
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
        conn = yield from create_connection(host=self.host, port=self.port,
                                            loop=self.loop)

        config = {'feature_negotiation':True, 'tls_v1': False,
                  'snappy': False, 'deflate': True
        }
        self.assertIsInstance(conn._parser, Reader)

        yield from conn.identify(**config)
        self.assertIsInstance(conn._parser, DeflateReader)
        yield from self._pub_sub_rdy_fin(conn)

    @asyncio.coroutine
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

    @run_until_complete
    def test_message(self):
        conn = yield from create_connection(host=self.host, port=self.port,
                                            loop=self.loop)

        ok = yield from conn.execute(b'PUB', self.topic, data=b'boom')
        self.assertEqual(ok, b'OK')
        yield from conn.execute(b'SUB', self.topic,  b'bar')
        yield from conn.execute(b'RDY', 1)


        msg = yield from conn._queue.get()
        self.assertEqual(msg.processed, False)

        yield from msg.touch()
        self.assertEqual(msg.processed, False)
        yield from msg.req(1)
        self.assertEqual(msg.processed, True)
        yield from conn.execute(b'RDY', 1)
        new_msg = yield from conn._queue.get()
        yield from new_msg.fin()
        self.assertEqual(msg.processed, True)

        yield from conn.execute(b'CLS')