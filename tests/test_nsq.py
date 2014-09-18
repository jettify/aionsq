import asyncio
from ._testutils import run_until_complete, BaseTest
from aionsq.connection import create_connection, NsqConnection
from aionsq.http import Nsqd
from aionsq.nsq import create_nsq


class NsqTest(BaseTest):

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
        nsq = yield from create_nsq(host=self.host, port=self.port,
                                    heartbeat_interval=30000,
                                    feature_negotiation=True,
                                    tls_v1=True,
                                    snappy=False,
                                    deflate=False,
                                    deflate_level=0,
                                    loop=self.loop)
        yield from nsq.pub(b'foo', b'bar')

        yield from nsq.sub(b'foo', b'bar')
        for waiter in nsq.wait_messages():
            yield from nsq.rdy(1)
            message = yield from waiter
            # import ipdb; ipdb.set_trace()
            yield from message.fin()
            break