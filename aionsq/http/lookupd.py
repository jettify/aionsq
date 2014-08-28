import asyncio
from .base import NsqHTTPConnection


class NsqLookupd(NsqHTTPConnection):
    """
    :see: http://nsq.io/components/nsqlookupd.html
    """

    @asyncio.coroutine
    def ping(self):
        """Monitoring endpoint.
        :returns: should return `"OK"`, otherwise raises an exception.
        """
        return self.perform_request('GET', 'ping', None, None)

    @asyncio.coroutine
    def info(self):
        """Returns version information."""
        response = yield from self.perform_request('GET', 'info', None, None)
        return response

    @asyncio.coroutine
    def lookup(self, topic):
        """XXX

        :param topic:
        :return:
        """
        response = yield from self.perform_request(
            'GET', 'lookup', {'topic': topic}, None)
        return response

    @asyncio.coroutine
    def topics(self):
        """XXX

        :return:
        """
        resp = yield from self.perform_request('GET', 'topics', None, None)
        return resp

    @asyncio.coroutine
    def channels(self, topic):
        """XXX

        :param topic:
        :return:
        """
        resp = yield from self.perform_request(
            'GET', 'channels', {'topic': topic}, None)
        return resp

    @asyncio.coroutine
    def nodes(self):
        """XXX

        :return:
        """
        resp = yield from self.perform_request('GET', 'nodes', None, None)
        return resp

    @asyncio.coroutine
    def delete_topic(self, topic):
        """XXX

        :param topic:
        :return:
        """
        resp = yield from self.perform_request(
            'GET', 'delete_topic', {'topic': topic}, None)
        return resp

    @asyncio.coroutine
    def delete_channel(self, topic, channel):
        """XXX

        :param topic:
        :param channel:
        :return:
        """
        resp = yield from self.perform_request(
            'GET', 'delete_channel', {'topic': topic, 'channel': channel},
            None)
        return resp

    @asyncio.coroutine
    def tombstone_topic_producer(self, topic, node):
        """XXX

        :param topic:
        :param node:
        :return:
        """
        resp = yield from self.perform_request(
            'GET', 'delete_channel', {'topic': topic, 'node': node},
            None)
        return resp
