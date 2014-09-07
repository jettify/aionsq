from ._testutils import run_until_complete, BaseTest
from aionsq.http.nsqd import Nsqd


class NsqdTest(BaseTest):
    """
    :see: http://nsq.io/components/nsqd.html
    """

    @run_until_complete
    def test_ok(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.ping()
        self.assertEqual(res, 'OK')

    @run_until_complete
    def test_info(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.info()
        self.assertIn('version', res)

    @run_until_complete
    def test_stats(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.stats()
        self.assertIn('version', res)

    @run_until_complete
    def test_pub(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.pub('baz', 'baz_msg')
        self.assertEqual('OK', res)

    @run_until_complete
    def test_mpub(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.mpub('baz', 'baz_msg:1', 'baz_msg:1')
        self.assertEqual('OK', res)

    @run_until_complete
    def test_create_topic(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.create_topic('foo2')
        self.assertEqual('', res)

    @run_until_complete
    def test_delete_topic(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.delete_topic('foo2')
        self.assertEqual('', res)

    @run_until_complete
    def test_create_channel(self):
        conn = Nsqd('127.0.0.1', 4151, loop=self.loop)
        res = yield from conn.create_topic('zap')
        self.assertEqual('', res)
        res = yield from conn.create_channel('zap', 'bar')
        self.assertEqual('', res)
