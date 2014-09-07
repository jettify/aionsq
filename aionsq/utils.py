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
