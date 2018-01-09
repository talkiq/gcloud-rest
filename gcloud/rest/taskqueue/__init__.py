from pkg_resources import get_distribution
__version__ = get_distribution('gcloud-rest').version

from gcloud.rest.taskqueue.error import FailFastError
from gcloud.rest.taskqueue.manager import TaskManager
from gcloud.rest.taskqueue.queue import TaskQueue
from gcloud.rest.taskqueue.utils import decode
from gcloud.rest.taskqueue.utils import encode


__all__ = ['__version__', 'FailFastError', 'TaskManager', 'TaskQueue',
           'decode', 'encode']
