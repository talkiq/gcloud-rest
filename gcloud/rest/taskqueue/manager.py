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


class TaskManager(object):
    # pylint: disable=too-many-instance-attributes
    def __init__(self, project, taskqueue, worker, backoff_base=2,
                 backoff_factor=1.1, backoff_max_value=60, batch_size=1,
                 burn=False, deadletter_insert_function=None,
                 google_api_lock=None, lease_seconds=60, retry_limit=None,
                 service_file=None):
        # pylint: disable=too-many-arguments
        self.worker = worker

        self.backoff = backoff(base=backoff_base, factor=backoff_factor,
                               max_value=backoff_max_value)
        self.batch_size = max(batch_size, 1)
        self.burn = burn
        self.deadletter_insert_function = deadletter_insert_function
        self.lease_seconds = lease_seconds
        self.retry_limit = retry_limit

        self.stop_event = multiprocessing.Event()

        self.google_api_lock = google_api_lock or multiprocessing.RLock()
        self.tq = TaskQueue(project, taskqueue, creds=service_file,
                            google_api_lock=self.google_api_lock)

    def find_tasks_forever(self):
        while not self.stop_event.is_set():
            try:
                churning = self.find_and_process_work()
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

            data = multiprocessing.Manager().dict()
            data['scheduleTime'] = task['scheduleTime']
            end_lease = multiprocessing.Event()

            lease_manager = multiprocessing.Process(
                target=self.lease_manager, args=(end_lease, task, data))
            lease_manager.start()

            leasers.append((end_lease, lease_manager, data))

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

            retries = task['status']['attemptDispatchCount']
            if self.retry_limit is None or retries < self.retry_limit:
                log.info('%d retries for task %s is below limit %d', retries,
                         task['name'], self.retry_limit)
                self.tq.cancel(task)
                return

            log.warning('retry_limit exceeded, failing task %s at %d',
                        task['name'], retries)
            self.fail_task(payload, result)
            self.tq.delete(task['name'])
            return

        self.tq.ack(task)

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

    def lease_manager(self, lease_event, task, data):
        """
        This function extends the Pull Task Queue lease to make sure no other
        workers pick up the same task. This is force-killed after the task
        work is complete
        """
        while not lease_event.is_set() and not self.stop_event.is_set():
            time.sleep(self.lease_seconds / 2)

            try:
                log.info('extending lease for %s', task['name'])

                task['scheduleTime'] = data['scheduleTime']
                renewed = self.tq.renew(task,
                                        lease_duration=self.lease_seconds)
                data['scheduleTime'] = renewed['scheduleTime']
            except Exception as e:  # pylint: disable=broad-except
                log.exception(e)
                # stop lease extension thread but not main thread
                lease_event.set()

    def stop(self):
        self.stop_event.set()
