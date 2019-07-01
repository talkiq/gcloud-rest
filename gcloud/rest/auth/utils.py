import base64
from typing import Union  # pylint: disable=unused-import


def decode(payload):
    # type: (str) -> bytes
    """
    Modified Base64 for URL variants exist, where the + and / characters of
    standard Base64 are respectively replaced by - and _.
    See https://en.wikipedia.org/wiki/Base64#URL_applications
    """
    return base64.b64decode(payload, altchars=b'-_')


def encode(payload):
    # type: (Union[bytes, str]) -> bytes
    """
    Modified Base64 for URL variants exist, where the + and / characters of
    standard Base64 are respectively replaced by - and _.
    See https://en.wikipedia.org/wiki/Base64#URL_applications
    """
    if isinstance(payload, str):
        payload = payload.encode('utf-8')

    return base64.b64encode(payload, altchars=b'-_')
