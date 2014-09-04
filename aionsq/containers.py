import asyncio
from collections import namedtuple


NsqErrorMessage = namedtuple('NsqError', ['code', 'msg'])
BaseMessage = namedtuple('NsqMessage',
    ['timestamp', 'attempts', 'message_id', 'body', 'conn'])


class NsqMessage(BaseMessage):

    @asyncio.coroutine
    def fin(self):
        """Finish a message (indicate successful processing)"""
        return (yield from self.conn.execute(b'FIN', self.message_id))

    @asyncio.coroutine
    def req(self, timeout=10):
        """Re-queue a message (indicate failure to process)

        :param timeout: ``int`` configured max timeout  0 is a special case
            that will not defer re-queueing
        """
        return (yield from self.conn.execute(b'REQ', self.message_id, timeout))

    @asyncio.coroutine
    def touch(self):
        """Reset the timeout for an in-flight message"""
        return (yield from self.conn.execute(b'TOUCH', self.message_id))
