from ._testutils import run_until_complete, BaseTest
from aionsq.http.lookupd import NsqLookupd


class NsqLookupdTest(BaseTest):
    """
    :see: http://nsq.io/components/nsqd.html
    """

    @run_until_complete
    def test_ok(self):
        conn = NsqLookupd(('127.0.0.1', 4161), loop=self.loop)
        res = yield from conn.ping()
        self.assertEqual(res, 'OK')

    @run_until_complete
    def test_info(self):
        conn = NsqLookupd(('127.0.0.1', 4161), loop=self.loop)
        res = yield from conn.info()
        self.assertTrue('version' in res)

    @run_until_complete
    def test_lookup(self):
        conn = NsqLookupd(('127.0.0.1', 4161), loop=self.loop)
        res = yield from conn.lookup('foo')
        self.assertIn('producers', res)

    @run_until_complete
    def test_topics(self):
        conn = NsqLookupd(('127.0.0.1', 4161), loop=self.loop)
        res = yield from conn.topics()
        self.assertEqual({'topics': ['foo']}, res)

    @run_until_complete
    def test_channels(self):
        conn = NsqLookupd(('127.0.0.1', 4161), loop=self.loop)
        res = yield from conn.channels('foo')
        self.assertEqual({'channels': ['bar']}, res)

