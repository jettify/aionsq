from ._testutils import run_until_complete, BaseTest
from aionsq.producer import create_producer, create_http_producer


class NsqTCPProducerTest(BaseTest):

    @run_until_complete
    def test_publish(self):

        endpoints = [('127.0.0.1', 4150)]
        config = {'tls_v1': False}
        nsq_producer = yield from create_producer(endpoints, config,
                                                  loop=self.loop)
        ok = yield from nsq_producer.publish('baz', 'producer msg')
        self.assertEqual(ok, b'OK')

    @run_until_complete
    def test_mpublish(self):

        endpoints = [('127.0.0.1', 4150)]
        config = {'tls_v1': False}
        nsq_producer = yield from create_producer(endpoints, config,
                                                  loop=self.loop)
        messages = ['baz:1', b'baz:2', 3.14, 42]
        ok = yield from nsq_producer.mpublish('baz', *messages)
        self.assertEqual(ok, b'OK')


class NsqHTTPProducerTest(BaseTest):

    @run_until_complete
    def test_http_publish(self):

        endpoints = [('127.0.0.1', 4151)]
        nsq_producer = yield from create_http_producer(endpoints,
                                                       loop=self.loop)
        ok = yield from nsq_producer.publish('http_baz', 'producer msg')
        self.assertEqual(ok, 'OK')

    @run_until_complete
    def test_http_mpublish(self):

        endpoints = [('127.0.0.1', 4151)]
        nsq_producer = yield from create_http_producer(endpoints,
                                                       loop=self.loop)
        messages = ['baz:1', b'baz:2', 3.14, 42]
        ok = yield from nsq_producer.mpublish('http_baz', *messages)
        self.assertEqual(ok, 'OK')
