from pkg_resources import get_distribution
__version__ = get_distribution('gcloud-rest').version

from gcloud.rest.auth.token import Token


__all__ = ['__version__', 'Token']
