import asyncio
import json
import ssl
import os
from aionsq.connection import create_connection

here = os.path.join(os.path.dirname(__file__), '..', 'tests')

keyfile = os.path.join(here, 'sample.key')
certfile = os.path.join(here, 'sample.crt')
sslcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
sslcontext.load_cert_chain(certfile, keyfile)
sslcontext.verify_mode = ssl.CERT_NONE


def main():

    loop = asyncio.get_event_loop()
    @asyncio.coroutine
    def go():
        # create tcp connection
        nsq = yield from create_connection(port=4150, loop=loop)
        # publish b'test_msg' to the topic: b'foo'
        import ipdb ; ipdb.set_trace()
        resp = yield from nsq.execute(
            b'IDENTIFY', data=json.dumps(
                {
                    "client_id": "metrics_increment",
                    "hostname": "localhost",
                    "heartbeat_interval": 30000,
                    "feature_negotiation": True,
                    "tls_v1": True
            }))
        # import ipdb; ipdb.set_trace()
        # resp = yield from nsq.execute(b'IDENTIFY', data=json.dumps({"tls_v1": True}))
        import ipdb; ipdb.set_trace()

        print(resp)
        yield from nsq._upgrade_connection()
        import ipdb; ipdb.set_trace()
        ok = yield from nsq.execute(b'PUB', b'foo', data=b'test_msg')
        # subscribe to the b'foo' topic and b'bar' channel
        import ipdb; ipdb.set_trace()

        yield from nsq.execute(b'SUB', b'foo', b'bar')
        # tell nsqd that we are reade receive 1 message
        import ipdb; ipdb.set_trace()

        yield from nsq.execute(b'RDY', b'1')
        # wait for message
        import ipdb; ipdb.set_trace()

        msg = yield from nsq._msq_queue.get()
        print(msg)
        # acknowledge message
        yield from nsq.execute(b'FIN', b'1')

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
# {"max_rdy_count":2500,
#  "version":"0.2.30",
#  "max_msg_timeout":900000,
#  "msg_timeout":0,
#  "tls_v1":false,
#  "deflate":false,
#  "deflate_level":0,
#  "max_deflate_level":6,
#  "snappy":false,
#  "sample_rate":0,
#  "auth_required":false}