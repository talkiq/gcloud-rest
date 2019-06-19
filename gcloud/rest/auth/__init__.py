from pkg_resources import get_distribution
__version__ = get_distribution('gcloud-rest').version

from gcloud.rest.auth.iam import IamClient
from gcloud.rest.auth.token import Token
from gcloud.rest.auth.utils import decode
from gcloud.rest.auth.utils import encode


__all__ = ['__version__', 'IamClient', 'Token', 'decode', 'encode']
