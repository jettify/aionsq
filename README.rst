aionsq (work in progress)
=========================

asyncio (PEP 3156) nsq_ (message queue) client.


Usage examples
--------------

Simple low-level interface:

.. code:: python

    import asyncio
    from aionsq.connection import create_connection


    def main():

        loop = asyncio.get_event_loop()
        @asyncio.coroutine
        def go():
            nsq = yield from create_connection(host='localhost', port=4150,
                                               loop=loop)
            data = yield from nsq.execute(b'PUB', b'test_topic', data=b'test_msg')
            print(data)
        loop.run_until_complete(go())


    if __name__ == '__main__':
        main()


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* nsq_


License
-------

The aionsq is offered under MIT license.

.. _Python: https://www.python.org
.. _asyncio: https://pypi.python.org/pypi/asyncio
.. _nsq: http://nsq.io
