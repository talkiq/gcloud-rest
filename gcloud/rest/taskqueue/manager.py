import datetime
import json
import logging
import multiprocessing
import time
import traceback

import requests

from gcloud.rest.core import backoff
from gcloud.rest.taskqueue.error import FailFastError
from gcloud.rest.taskqueue.queue import TaskQueue
from gcloud.rest.taskqueue.utils import decode


log = logging.getLogger(__name__)


class BrokenTaskManagerException(Exception):
    pass


def lease_manager(project, taskqueue, creds, google_api_lock, event, task,
                  lease_seconds, data):
    # pylint: disable=too-many-arguments
    """
    This function extends the Pull Task Queue lease to make sure no other
    workers pick up the same task. This is force-killed after the task work
    is complete
    """
    tq = TaskQueue(project, taskqueue, creds=creds,
                   google_api_lock=google_api_lock)

    while not event.is_set():
        for _ in range(int(lease_seconds // 2) * 10):
            time.sleep(0.1)
            if event.is_set():
                break

        try:
            log.info('extending lease for %s', task['name'])

            task['scheduleTime'] = data['scheduleTime']
            renewed = tq.renew(task, lease_duration=lease_seconds)
            data['scheduleTime'] = renewed['scheduleTime']
        except Exception as e:  # pylint: disable=broad-except
            log.error('failed to autorenew task: %s', task['name'])
            log.exception(e)
            event.set()


class TaskManager(object):
    # pylint: disable=too-many-instance-attributes
    def __init__(self, project, taskqueue, worker, backoff_base=2,
                 backoff_factor=1.1, backoff_max_value=60, batch_size=1,
                 burn=False, deadletter_insert_function=None,
                 google_api_lock=None, lease_seconds=60, retry_limit=None,
                 service_file=None):
        # pylint: disable=too-many-arguments
        self.project = project
        self.taskqueue = taskqueue
        self.worker = worker
        self.creds = service_file

        self.backoff = backoff(base=backoff_base, factor=backoff_factor,
                               max_value=backoff_max_value)
        self.batch_size = max(batch_size, 1)
        self.burn = burn
        self.deadletter_insert_function = deadletter_insert_function
        self.lease_seconds = lease_seconds
        self.retry_limit = retry_limit

        self.manager = multiprocessing.Manager()
        self.stop_event = multiprocessing.Event()

        self.google_api_lock = google_api_lock or multiprocessing.RLock()
        self.tq = TaskQueue(project, taskqueue, creds=self.creds,
                            google_api_lock=self.google_api_lock)

    def find_tasks_forever(self):
        while not self.stop_event.is_set():
            try:
                churning = self.find_and_process_work()
            except BrokenTaskManagerException:
                raise
            except Exception as e:  # pylint: disable=broad-except
                log.exception(e)
                continue

            if churning:
                time.sleep(next(self.backoff))
            else:
                self.backoff.send(None)
                self.backoff.send('reset')

    def find_and_process_work(self):
        """
        Query the Pull Task Queue REST API for work every N seconds. If work
        found, block and perform work while asynchronously updating the lease
        deadline.

        http://stackoverflow.com/a/17071255
        """
        # pylint: disable=too-many-locals
        try:
            task_lease = self.tq.lease(num_tasks=self.batch_size,
                                       lease_duration=self.lease_seconds)
        except requests.exceptions.HTTPError as e:
            log.exception(e)
            return True

        if not task_lease:
            return True

        tasks = task_lease.get('tasks')
        log.info('grabbed %d tasks', len(tasks))

        if self.burn:
            to_burn = tasks[1:]
            tasks = [tasks[0]]

            for task in to_burn:
                log.info('burning task %s', task.get('name'))
                self.tq.delete(task.get('name'))

        leasers = []
        payloads = []
        for task in tasks:
            payloads.append(
                json.loads(decode(task['pullMessage']['payload']).decode()))

            try:
                data = self.manager.dict()
                data['scheduleTime'] = task['scheduleTime']
                event = multiprocessing.Event()

                lm = multiprocessing.Process(
                    target=lease_manager,
                    args=(self.project, self.taskqueue, self.creds,
                          self.google_api_lock, event, task,
                          self.lease_seconds, data))
                lm.daemon = True
                lm.start()
            except Exception as e:
                log.exception(e)
                raise BrokenTaskManagerException('broken process pool')

            leasers.append((event, lm, data))

        try:
            results = self.worker(payloads)
        except Exception:
            # Ensure subprocesses die. N.B. doing this in multiple loops is
            # overall faster, since we don't care about the renewed tasks.
            for (e, _, _) in leasers:
                e.set()
            for (_, lm, _) in leasers:
                lm.join()
            raise

        for ((e, lm, data), task, payload, result) in zip(leasers, tasks,
                                                          payloads, results):
            e.set()
            lm.join()
            self.check_task_result(task, data, payload, result)

        return False

    def check_task_result(self, task, data, payload, result):
        task['scheduleTime'] = data['scheduleTime']

        if isinstance(result, FailFastError):
            log.error('[FailFastError] failed to process task: %s', payload)
            log.exception(result)

            self.fail_task(payload, result)
            self.tq.cancel(task)
            return

        if isinstance(result, Exception):
            log.error('failed to process task: %s', payload)
            log.exception(result)

            if self.retry_limit is None:
                self.tq.cancel(task)
                return

            retries = int(task['status']['attemptDispatchCount'])
            if retries < self.retry_limit:
                log.info('%d retries for task %s is below limit %d', retries,
                         task['name'], self.retry_limit)
                self.tq.cancel(task)
                return

            log.warning('retry_limit exceeded, failing task %s at %d',
                        task['name'], retries)
            self.fail_task(payload, result)
            self.tq.delete(task['name'])
            return

        log.info('successfully processed task: %s', task['name'])
        self.tq.ack(task)

    def fail_task(self, payload, exception):
        if not self.deadletter_insert_function:
            return

        properties = {
            'error': str(exception),
            'generation': None,
            'metageneration': None,
            'payload': payload,
            'time_created': datetime.datetime.now(),
            'traceback': traceback.format_exc(),
            'update': None,
        }

        self.deadletter_insert_function(payload.get('name'), properties)

    def stop(self):
        self.stop_event.set()
