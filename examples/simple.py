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
