from pkg_resources import get_distribution
__version__ = get_distribution('gcloud-rest').version

from gcloud.rest.kms.client import KMS


__all__ = ['__version__', 'KMS']
