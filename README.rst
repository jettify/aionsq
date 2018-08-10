aionsq (Not Maintained )
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
            # create tcp connection
            nsq = yield from create_connection(port=4150, loop=loop)
            # publish b'test_msg' to the topic: b'foo'
            ok = yield from nsq.execute(b'PUB', b'foo', data=b'test_msg')
            # subscribe to the b'foo' topic and b'bar' channel
            yield from nsq.execute(b'SUB', b'foo', b'bar')
            # tell nsqd that we are reade receive 1 message
            yield from nsq.execute(b'RDY', b'1')
            # wait for message
            msg = yield from nsq._msq_queue.get()
            print(msg)
            # acknowledge message
            yield from nsq.execute(b'FIN', b'1')

        loop.run_until_complete(go())

    if __name__ == '__main__':
        main()


High-level interface for one nsq connection:

.. code:: python

    import asyncio
    from aionsq.nsq import create_nsq


    def main():

        loop = asyncio.get_event_loop()
        @asyncio.coroutine
        def go():
            nsq = yield from create_nsq(host='127.0.0.1', port=4150, loop=loop)
            yield from nsq.pub(b'foo', b'msg foo')
            yield from nsq.sub(b'foo', b'bar')
            yield from nsq.rdy(1)
            msg = yield from nsq.wait_messages()
            print(msg)
            yield from msg.fin()
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
