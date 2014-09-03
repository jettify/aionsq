import asyncio
from aionsq.connection import create_connection


def main():

    loop = asyncio.get_event_loop()
    @asyncio.coroutine
    def go():
        nsq = yield from create_connection(port=4150, loop=loop)
        resp = yield from nsq.identify(**{
                    "client_id": "metrics_increment",
                    "hostname": "localhost",
                    "heartbeat_interval": 30000,
                    "feature_negotiation": True,
                    # "tls_v1": True,
                    # "snappy": True,
                    # "sample_rate": 50,
                    # "deflate": True, "deflate_level": 6,
            })
        print(resp)
        yield from nsq.pub(b'foo', b'msg foo')
        yield from nsq.sub(b'foo', b'bar')
        yield from nsq.rdy(1)
        msg = yield from nsq._msq_queue.get()
        yield from nsq.fin(msg.message_id)
        print(msg)
        yield from nsq.close()

        import ipdb; ipdb.set_trace()
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
