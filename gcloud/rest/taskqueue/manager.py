import datetime
import json
import logging
import os
import threading
import time
import traceback

import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from gcloud.rest.core import backoff
from gcloud.rest.taskqueue.error import FailFastError
from gcloud.rest.taskqueue.queue import TaskQueue
from gcloud.rest.taskqueue.utils import clean_b64decode


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
        self.project = project
        self.taskqueue = taskqueue
        self.task_worker = task_worker

        self.backoff = backoff(base=backoff_base, factor=backoff_factor,
                               max_value=backoff_max_value)
        self.batch_size = batch_size
        self.burn = burn
        self.deadletter_insert_function = deadletter_insert_function
        self.google_api_lock = google_api_lock or threading.RLock()
        self.lease_seconds = lease_seconds
        self.retry_limit = retry_limit

        with self.google_api_lock:
            # TODO: move this functionality into TaskQueue (lease and patch)
            self.tasks_api = self.init_tasks_api(service_file=service_file)

        self.stop_event = threading.Event()

        self.tq = TaskQueue(project, taskqueue, creds=service_file,
                            google_api_lock=self.google_api_lock)

    @staticmethod
    def init_tasks_api(service_file=None):
        creds = service_file or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not creds:
            raise Exception('could not load service credentials')

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            creds, scopes=SCOPES)
        credentials_http = credentials.authorize(httplib2.Http(timeout=30))
        service = build('taskqueue', 'v1beta2', http=credentials_http)
        return service.tasks()

    def find_tasks_forever(self):
        while not self.stop_event.is_set():
            self.find_and_process_work()
            try:
                self.backoff.send('reset')
            except TypeError:
                # a TypeError is thrown when attempting to `send()` to a newly-
                # created generator
                pass

    def find_and_process_work(self):
        """
        Query the Pull Task Queue REST API for work every N seconds. If work
        found, block and perform work while asynchronously updating the lease
        deadline.

        http://stackoverflow.com/a/17071255
        """
        try:
            with self.google_api_lock:
                task_lease = self.tasks_api.lease(
                    project=self.project,
                    taskqueue=self.taskqueue,
                    numTasks=self.batch_size,
                    leaseSecs=self.lease_seconds
                ).execute()

            tasks = task_lease.get('items')
            if not tasks:
                time.sleep(next(self.backoff))
                return

            log.info('grabbed %d tasks', len(tasks))

            if self.burn:
                to_burn = tasks[1:]
                tasks = [tasks[0]]

                for task in to_burn:
                    log.info('burning task %s', task.get('id'))
                    self.delete_task(task)

            end_lease_events = []
            payloads = []
            for task in tasks:
                data = clean_b64decode(task['payloadBase64'])
                payload = json.loads(data)
                end_lease_event = threading.Event()

                threading.Thread(
                    target=self.lease_manager,
                    args=(end_lease_event, task)
                ).start()

                payloads.append(payload)
                end_lease_events.append(end_lease_event)

            results = self.task_worker(payloads)

            for i, result in enumerate(results):
                if isinstance(result, FailFastError):
                    log.error('[FailFastError] failed to process task: %s',
                              str(payloads[i]))
                    log.exception(result)

                    self.fail_task(payloads[i], result)
                    self.delete_task(tasks[i])
                elif isinstance(result, Exception):
                    log.error('failed to process task: %s', str(payloads[i]))
                    log.exception(result)

                    if self.retry_limit is not None and \
                            tasks[i]['retry_count'] >= self.retry_limit:
                        log.warning('exceeded retry_limit, failing task')
                        self.fail_task(payloads[i], result)
                        self.delete_task(tasks[i])
                else:
                    self.delete_task(tasks[i])

            for e in end_lease_events:
                e.set()
        except Exception as e:
            log.exception(e)
            self.stop_event.set()
            raise

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
            patch_body = {
                'kind': 'taskqueues#task',
                'id': task['id'],
                'queueName': self.taskqueue
            }

            try:
                log.info('extending lease for %s', task['id'])

                with self.google_api_lock:
                    self.tasks_api.patch(
                        project=self.project,
                        taskqueue=self.taskqueue,
                        task=task['id'],
                        body=patch_body,
                        newLeaseSeconds=self.lease_seconds
                    ).execute()
            except Exception as e:  # pylint: disable=broad-except
                log.exception(e)
                # stop lease extension thread but not main thread
                lease_event.set()
                break

            time.sleep(self.lease_seconds / 2)

    def delete_task(self, task):
        threading.Thread(
            target=self.tq.delete,
            args=(task.get('id'),)
        ).start()

    def stop(self):
        self.stop_event.set()
