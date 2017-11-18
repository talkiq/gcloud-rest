from pkg_resources import get_distribution
__version__ = get_distribution('gcloud-rest').version

from gcloud.rest.taskqueue.error import FailFastError
from gcloud.rest.taskqueue.manager import TaskManager
from gcloud.rest.taskqueue.queue import TaskQueue


__all__ = ['__version__', 'FailFastError', 'TaskManager', 'TaskQueue']
