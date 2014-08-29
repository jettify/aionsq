from ._testutils import run_until_complete, BaseTest
from aionsq.connection import create_connection, NsqConnection


class NsqConnectionTest(BaseTest):

    @run_until_complete
    def test_basic_instance(self):
        nsq = yield from create_connection(port=4150, loop=self.loop)
        self.assertIsInstance(nsq, NsqConnection)
        self.assertTrue('NsqConnection' in nsq.__repr__())
        self.assertTrue(not nsq.closed)

