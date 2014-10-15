import asyncio
from aionsq.nsq import create_nsq


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        nsq = yield from create_nsq(host='127.0.0.1', port=4150,
                                    heartbeat_interval=30000,
                                    feature_negotiation=True,
                                    tls_v1=False,
                                    # snappy=True,
                                    # deflate=True,
                                    deflate_level=0,
                                    loop=loop)

        yield from nsq.pub(b'foo', b'msg foo')
        yield from nsq.sub(b'foo', b'bar')
        yield from nsq.rdy(1)
        msg = yield from nsq.wait_messages()
        assert not msg.processed
        yield from msg.fin()
        print(msg)
        assert msg.processed
        yield from nsq.cls()


    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
