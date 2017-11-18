import base64


def clean_b64decode(payload):
    """
    https://en.wikipedia.org/wiki/Base64#URL_applications

    Modified Base64 for URL variants exist, where the + and / characters
    of standard Base64 are respectively replaced by - and _
    """
    return base64.b64decode(payload.replace('-', '+').replace('_', '/'))
