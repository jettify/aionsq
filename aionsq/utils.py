import random
import re

TOPIC_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+$')
CHANNEL_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')


def valid_topic_name(topic):
    if not 0 < len(topic) < 33:
        return False
    return bool(TOPIC_NAME_RE.match(topic))


def valid_channel_name(channel):
    if not 0 < len(channel) < 33:
        return False
    return bool(CHANNEL_NAME_RE.match(channel))


_converters_to_bytes = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
    }


_converters_to_str = {
    str: lambda val: val,
    bytearray: lambda val: bytes(val).decode('utf-8'),
    bytes: lambda val: val.decode('utf-8'),
    int: lambda val: str(val),
    float: lambda val: str(val),
    }


def _convert_to_bytes(value):
    if type(value) in _converters_to_bytes:
        converted_value = _converters_to_bytes[type(value)](value)
    else:
        raise TypeError("Argument {!r} expected to be of bytes,"
                        " str, int or float type".format(value))
    return converted_value


def _convert_to_str(value):
    if type(value) in _converters_to_str:
        converted_value = _converters_to_str[type(value)](value)
    else:
        raise TypeError("Argument {!r} expected to be of bytes,"
                        " str, int or float type".format(value))
    return converted_value


def retry_iterator(init_delay=0.1, max_delay=10.0,
                    factor=2.7182818284590451,
                    jitter=0.11962656472, max_retries=None, now=True):
    """Based on twisted reconnection factory.

    :param init_delay:
    :param max_delay:
    :param factor:
    :param jitter:
    :param max_retries:
    :param now:
    :return:
    """
    retries, delay = 0, init_delay
    if now:
        retries += 1
        yield 0.0
    while not max_retries or retries < max_retries:
        retries += 1
        delay *= factor
        delay = random.normalvariate(delay, delay * jitter) if jitter else delay
        delay = min(delay, max_delay) if max_delay else delay
        yield delay
    else:
        raise MaxRetriesExided()