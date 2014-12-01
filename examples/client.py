import asyncio
from aionsq.consumer import NsqConsumer
from aionsq.nsq import create_nsq


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        nsq_producer = yield from create_nsq(host='localhost', port=4150,
                            heartbeat_interval=30000,
                            feature_negotiation=True,
                            tls_v1=True,
                            snappy=False,
                            deflate=False,
                            deflate_level=0,
                            loop=loop)
        for i in range(0, 35):
            yield from nsq_producer.pub(b'foo', 'xxx:{}'.format(i))


        endpoints = [('localhost', 4150)]
        nsq_consumer = NsqConsumer(nsqd_tcp_addresses=endpoints, loop=loop)
        yield from nsq_consumer.connect()
        yield from nsq_consumer.subscribe(b'foo', b'bar')
        for waiter in nsq_consumer.wait_messages():
            message = yield from waiter
            print(message)
            yield from message.fin()

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
