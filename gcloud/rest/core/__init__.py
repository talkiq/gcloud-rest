from pkg_resources import get_distribution
__version__ = get_distribution('gcloud-rest').version

from gcloud.rest.core.util import backoff
from gcloud.rest.core.util import decode
from gcloud.rest.core.util import encode


__all__ = ['__version__', 'backoff', 'decode', 'encode']
