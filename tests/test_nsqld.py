from ._testutils import run_until_complete, BaseTest
from aionsq.http.nsqd import Nsqd


class NsqdTest(BaseTest):
    """
    :see: http://nsq.io/components/nsqd.html
    """

    @run_until_complete
    def test_ok(self):
        conn = Nsqd(('127.0.0.1', 4151), loop=self.loop)
        res = yield from conn.ping()
        self.assertEqual(res, 'OK')

    @run_until_complete
    def test_info(self):
        conn = Nsqd(('127.0.0.1', 4151), loop=self.loop)
        res = yield from conn.info()
        self.assertIn('version', res),

    @run_until_complete
    def test_stats(self):
        conn = Nsqd(('127.0.0.1', 4151), loop=self.loop)
        res = yield from conn.stats()
        self.assertIn('version', res),
