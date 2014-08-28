import asyncio
import json
from .base import NsqHTTPConnection


class Nsqd(NsqHTTPConnection):
    """
    :see: http://nsq.io/components/nsqd.html
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
        resp = yield from self.perform_request('GET', 'info', None, None)
        return resp

    @asyncio.coroutine
    def stats(self):
        """Returns version information."""
        resp = yield from self.perform_request(
            'GET', 'stats', {'format': 'json'}, None)
        return resp

    @asyncio.coroutine
    def pub(self, topic, message):
        """Returns version information."""
        resp = yield from self.perform_request(
            'POST', 'pub', {'topic': topic}, message)
        return resp

    @asyncio.coroutine
    def mpub(self, topic, *messages):
        """Returns version information."""
        assert len(messages), "Specify one or mor message"
        msgs = '\n'.join(messages)
        resp = yield from self.perform_request(
            'POST', 'mpub', {'topic': topic}, msgs)
        return resp

    @asyncio.coroutine
    def create_topic(self, topic):
        resp = yield from self.perform_request(
            'POST', 'topic/create', {'topic': topic}, None)
        return resp

    @asyncio.coroutine
    def delete_topic(self, topic):
        resp = yield from self.perform_request(
            'POST', 'topic/delete', {'topic': topic}, None)
        return resp

    @asyncio.coroutine
    def create_channel(self, topic, channel):
        resp = yield from self.perform_request(
            'POST', 'channel/create', {'topic': topic, 'channel': channel},
            None)
        return resp

    @asyncio.coroutine
    def delete_channel(self, topic, channel):
        resp = yield from self.perform_request(
            'GET', 'channel/delete', {'topic': topic, 'channel': channel},
            None)
        return resp

    @asyncio.coroutine
    def empty_topic(self, topic):
        resp = yield from self.perform_request(
            'GET', 'topic/empty', {'topic': topic}, None)
        return resp

    @asyncio.coroutine
    def topic_pause(self, topic):
        resp = yield from self.perform_request(
            'GET', 'topic/pause', {'topic': topic}, None)
        return resp

    @asyncio.coroutine
    def topic_unpause(self, topic):
        resp = yield from self.perform_request(
            'GET', 'topic/unpause', {'topic': topic}, None)
        return resp

    @asyncio.coroutine
    def pause_channel(self, channel, topic):
        resp = yield from self.perform_request(
            'GET', 'channel/pause', {'topic': topic, 'channel': channel},
            None)
        return resp

    @asyncio.coroutine
    def unpause_channel(self, channel, topic):
        resp = yield from self.perform_request(
            'GET', '/channel/unpause', {'topic': topic, 'channel': channel},
            None)
        return resp

    @asyncio.coroutine
    def debug_pprof(self):
        resp = yield from self.perform_request(
            'GET', 'debug/pprof', None, None)
        return resp

    @asyncio.coroutine
    def debug_pprof_profile(self):
        resp = yield from self.perform_request(
            'GET', 'debug/pprof/profile', None, None)
        return resp

    @asyncio.coroutine
    def debug_pprof_goroutine(self):
        resp = yield from self.perform_request(
            'GET', '/debug/pprof/goroutine', None, None)
        return resp

    @asyncio.coroutine
    def debug_pprof_heap(self):
        resp = yield from self.perform_request(
            'GET', '/debug/pprof/heap', None, None)
        return resp


    @asyncio.coroutine
    def debug_pprof_block(self):
        resp = yield from self.perform_request(
            'GET', '/debug/pprof/block', None, None)
        return resp

    @asyncio.coroutine
    def debug_pprof_threadcreate(self):
        resp = yield from self.perform_request(
            'GET', '/debug/pprof/threadcreate', None, None)
        return resp
