from ._testutils import run_until_complete, BaseTest
from aionsq.connection import create_connection, NsqConnection


class NsqConnectionTest(BaseTest):

    @run_until_complete
    def test_basic_instance(self):
        host, port = '127.0.0.1', 4150
        nsq = yield from create_connection(host=host, port=port,
                                           loop=self.loop)
        self.assertIsInstance(nsq, NsqConnection)
        self.assertTrue('NsqConnection' in nsq.__repr__())
        self.assertTrue(not nsq.closed)
        self.assertTrue(host in nsq.id)
        self.assertTrue(str(port) in nsq.id)

