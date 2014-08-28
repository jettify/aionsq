import asyncio
from collections import namedtuple


NsqErrorMessage = namedtuple('NsqError', ['code', 'msg'])
BaseMessage = namedtuple('NsqMessage',
    ['timestamp', 'attempts', 'message_id', 'message', 'conn'])


class NsqMessage(BaseMessage):

    @asyncio.coroutine
    def fin(self):
        """XXX"""
        return (yield from self.conn.fin(self.message_id))

    @asyncio.coroutine
    def req(self, timeout=10):
        """XXX

        :param timeout:
        """
        return (yield from self.conn.req(self.message_id, timeout))

    @asyncio.coroutine
    def touch(self):
        """XXX"""
        return (yield from self.conn.touch(self.message_id))


class NsqMessageSecondVariant(object):
    """XXX"""

    __slots__ = ('timestamp', 'attempts', 'message_id', 'message', '_conn')

    def __init__(self, timestamp, attempts, message_id, message, conn):
        self.timestamp = timestamp
        self.attempts = attempts
        self.message_id = message_id
        self.message = message
        self._conn = conn

    @asyncio.coroutine
    def fin(self):
        """XXX"""
        return (yield from self.conn.fin(self.message_id))

    @asyncio.coroutine
    def req(self, timeout=10):
        """XXX

        :param timeout:
        """
        return (yield from self.conn.req(self.message_id, timeout))

    @asyncio.coroutine
    def touch(self):
        """XXX"""
        return (yield from self.conn.touch(self.message_id))

    def __repr__(self):
        return '<NsqProducer [db:{}]>'.format(self._db)
