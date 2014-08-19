import asyncio
from aionsq.connection import create_connection



import asyncio


def main():

    loop = asyncio.get_event_loop()
    @asyncio.coroutine
    def go():
        nsq = yield from create_connection(port=4150, loop=loop)
        data = yield from nsq.execute(b'PUB', b'test', data=b'test_topic')
        print(data)
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
