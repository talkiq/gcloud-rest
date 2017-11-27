from pkg_resources import get_distribution
__version__ = get_distribution('gcloud-rest').version

from gcloud.rest.storage.bucket import Bucket


__all__ = ['__version__', 'Bucket']
