import datetime
import json
import logging
import threading
import time
import traceback

from gcloud.rest.core import backoff
from gcloud.rest.taskqueue.error import FailFastError
from gcloud.rest.taskqueue.queue import TaskQueue
from gcloud.rest.taskqueue.utils import decode


log = logging.getLogger(__name__)

# https://github.com/google/google-api-python-client/issues/299
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.CRITICAL)

SCOPES = [
    'https://www.googleapis.com/auth/taskqueue',
    'https://www.googleapis.com/auth/taskqueue.consumer',
]


class TaskManager(object):
    # pylint: disable=too-many-instance-attributes
    def __init__(self, project, taskqueue, task_worker, backoff_base=2,
                 backoff_factor=1.1, backoff_max_value=60, batch_size=1,
                 burn=False, deadletter_insert_function=None,
                 google_api_lock=None, lease_seconds=60, retry_limit=None,
                 service_file=None):
        # pylint: disable=too-many-arguments
        self.task_worker = task_worker

        self.backoff = backoff(base=backoff_base, factor=backoff_factor,
                               max_value=backoff_max_value)
        self.batch_size = batch_size
        self.burn = burn
        self.deadletter_insert_function = deadletter_insert_function
        self.lease_seconds = lease_seconds
        self.retry_limit = retry_limit

        self.stop_event = threading.Event()

        self.google_api_lock = google_api_lock or threading.RLock()
        self.tq = TaskQueue(project, taskqueue, creds=service_file,
                            google_api_lock=self.google_api_lock)

    def find_tasks_forever(self):
        while not self.stop_event.is_set():
            self.find_and_process_work()
            self.backoff.send(None)
            self.backoff.send('reset')

    def find_and_process_work(self):
        """
        Query the Pull Task Queue REST API for work every N seconds. If work
        found, block and perform work while asynchronously updating the lease
        deadline.

        http://stackoverflow.com/a/17071255
        """
        try:
            task_lease = self.tq.lease(num_tasks=self.batch_size,
                                       lease_duration=self.lease_seconds)
            if not task_lease:
                time.sleep(next(self.backoff))
                return

            tasks = task_lease.get('tasks')
            log.info('grabbed %d tasks', len(tasks))

            if self.burn:
                to_burn = tasks[1:]
                tasks = [tasks[0]]

                for task in to_burn:
                    log.info('burning task %s', task.get('name'))
                    threading.Thread(target=self.tq.delete,
                                     args=(task.get('name'),)).start()

            end_lease_events = []
            payloads = []
            for task in tasks:
                data = decode(task['pullMessage']['payload']).decode()
                payload = json.loads(data)
                end_lease_event = threading.Event()

                threading.Thread(
                    target=self.lease_manager,
                    args=(end_lease_event, task)
                ).start()

                payloads.append(payload)
                end_lease_events.append(end_lease_event)

            results = self.task_worker(payloads)

            for task, payload, result in zip(tasks, payloads, results):
                self.check_task_result(task, payload, result)

            for e in end_lease_events:
                e.set()
        except Exception as e:
            log.exception(e)
            self.stop_event.set()
            raise

    def check_task_result(self, task, payload, result):
        if isinstance(result, FailFastError):
            log.error('[FailFastError] failed to process task: %s', payload)
            log.exception(result)

            self.fail_task(payload, result)
            threading.Thread(target=self.tq.cancel, args=(task,)).start()
            return

        if isinstance(result, Exception):
            log.error('failed to process task: %s', payload)
            log.exception(result)

            retries = task['status']['attemptDispatchCount']
            if self.retry_limit is None or retries < self.retry_limit:
                threading.Thread(target=self.tq.cancel, args=(task,)).start()
                return

            log.warning('exceeded retry_limit, failing task')
            self.fail_task(payload, result)
            threading.Thread(target=self.tq.delete,
                             args=(task.get('name'),)).start()
            return

        threading.Thread(target=self.tq.ack, args=(task,)).start()

    def fail_task(self, payload, exception):
        if not self.deadletter_insert_function:
            return

        properties = {
            'bucket': payload.get('bucket'),
            'error': str(exception),
            'generation': None,
            'metageneration': None,
            'time_created': datetime.datetime.now(),
            'traceback': traceback.format_exc(exception),
            'update': None,
        }

        self.deadletter_insert_function(payload.get('name'), properties)

    def lease_manager(self, lease_event, task):
        """
        This function extends the Pull Task Queue lease to make sure no other
        workers pick up the same task. This is force-killed after the task
        work is complete
        """
        time.sleep(self.lease_seconds / 2)

        while not lease_event.is_set() and not self.stop_event.is_set():
            try:
                log.info('extending lease for %s', task['name'])
                self.tq.renew(task, lease_duration=self.lease_seconds)
            except Exception as e:  # pylint: disable=broad-except
                log.exception(e)
                # stop lease extension thread but not main thread
                lease_event.set()
                break

            time.sleep(self.lease_seconds / 2)

    def stop(self):
        self.stop_event.set()
