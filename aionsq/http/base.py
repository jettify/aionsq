import asyncio
import json
import logging

import aiohttp

logger = logging.getLogger(__name__)


class NsqHTTPConnection:
    """XXX"""

    def __init__(self, endpoint, *, loop):
        self._loop = loop
        self._endpoint = endpoint
        self._connector = aiohttp.TCPConnector(resolve=True, loop=loop)
        self._base_url = 'http://{0}:{1}/'.format(*endpoint)
        self._request = aiohttp.request

    @property
    def endpoint(self):
        return self._endpoint

    def close(self):
        self._connector.close()

    @asyncio.coroutine
    def perform_request(self, method, url, params, body):

        url = self._base_url + url
        resp = yield from self._request(method, url, params=params,
                                        data=body, loop=self._loop,
                                        connector=self._connector)
        resp_body = yield from resp.text()
        try:
            response = json.loads(resp_body)
        except ValueError:
            return resp_body

        if not (200 <= resp.status <= 300):
            exc_class = Exception
            raise exc_class(resp.status, resp_body, response)
        return response['data']


    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<{}: {}>'.format(cls_name, self._endpoint)