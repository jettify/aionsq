import asyncio
from aionsq.connection import create_connection


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        nsq = yield from create_connection(port=4150, loop=loop)
        resp = yield from nsq.identify(
            **{
                "client_id": "metrics_increment",
                "hostname": "localhost",
                "heartbeat_interval": 30000,
                "feature_negotiation": True,
                "tls_v1": True,
                # "snappy": True,
                # "sample_rate": 50,
                "deflate": True, "deflate_level": 6,
            })
        # resp = yield from nsq.execute(b'IDENTIFY',
        # data=json.dumps({"tls_v1": True}))

        print(resp)
        for i in range(0, 100):
            d = b'test_msg: ' + bytes([i])
            print('send ', i, '-----', d)
            yield from nsq.execute(b'PUB', b'foo', data=d)

        yield from nsq.execute(b'SUB', b'foo', b'bar')

        for i in range(0, 50):
            yield from nsq.execute(b'RDY', b'1')
            msg = yield from nsq._msq_queue.get()
            yield from nsq.execute(b'FIN', msg.message_id)

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
