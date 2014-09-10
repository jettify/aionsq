import asyncio
from aionsq.consumer import NsqConsumer


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        endpoints = [('localhost', 4150)]
        nsq = NsqConsumer(nsqd_tcp_addresses=endpoints, loop=loop)
        yield from nsq.connect()

        yield from nsq.publish(b'foo', b'bar')

        yield from nsq.subscribe(b'foo', b'bar')
        for waiter in nsq.wait_messages():
            message = yield from waiter
            yield from message.fin()

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
