import datetime
import json
import logging
import multiprocessing
import threading
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

        self.stop_event = threading.Event()
        self.tasks = dict()

        self.google_api_lock = google_api_lock or threading.RLock()
        self.tq = TaskQueue(project, taskqueue, creds=service_file,
                            google_api_lock=self.google_api_lock)

    def find_tasks_forever(self):
        while not self.stop_event.is_set():
            try:
                churning = self.find_and_process_work()
            except Exception as e:
                log.exception(e)
                raise

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
                threading.Thread(target=self.tq.delete,
                                 args=(task.get('name'),)).start()

        leasers = []
        payloads = []
        for task in tasks:
            self.tasks[task['name']] = task
            payloads.append(
                json.loads(decode(task['pullMessage']['payload']).decode()))

            end_lease = multiprocessing.Event()
            lease_manager = multiprocessing.Process(
                target=self.lease_manager, args=(end_lease, task['name']))
            lease_manager.start()
            leasers.append((end_lease, lease_manager))

        try:
            results = self.worker(payloads)
        finally:
            for (e, _) in leasers:
                e.set()
            for (_, lm) in leasers:
                lm.join()

        for task, payload, result in zip(tasks, payloads, results):
            self.check_task_result(task['name'], payload, result)

    def check_task_result(self, tname, payload, result):
        task = self.tasks[tname]
        del self.tasks[tname]

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
                log.info('%d retries for task %s is below limit %d', retries,
                         tname, self.retry_limit)
                threading.Thread(target=self.tq.cancel, args=(task,)).start()
                return

            log.warning('retry_limit exceeded, failing task %s at %d', tname,
                        retries)
            self.fail_task(payload, result)
            threading.Thread(target=self.tq.delete,
                             args=(task['name'],)).start()
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

    def lease_manager(self, lease_event, tname):
        """
        This function extends the Pull Task Queue lease to make sure no other
        workers pick up the same task. This is force-killed after the task
        work is complete
        """
        while not lease_event.is_set() and not self.stop_event.is_set():
            time.sleep(self.lease_seconds / 2)

            try:
                log.info('extending lease for %s', tname)
                renewed = self.tq.renew(self.tasks[tname],
                                        lease_duration=self.lease_seconds)
                self.tasks[tname] = renewed
            except Exception as e:  # pylint: disable=broad-except
                log.exception(e)
                # stop lease extension thread but not main thread
                lease_event.set()

    def stop(self):
        self.stop_event.set()
