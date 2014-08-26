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
        response = yield from self.perform_request('GET', 'info', None, None)
        return response

    @asyncio.coroutine
    def stats(self):
        """Returns version information."""
        response = yield from self.perform_request(
            'GET', 'stats', {'format': 'json'}, None)
        return response
