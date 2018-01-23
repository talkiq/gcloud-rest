import logging
import threading

import requests

from gcloud.rest.auth import Token


log = logging.getLogger(__name__)

API_ROOT = 'https://cloudtasks.googleapis.com/v2beta2'
LOCATION = 'us-central1'
SCOPES = [
    'https://www.googleapis.com/auth/cloud-tasks',
]


def raise_for_status(resp):
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            log.error(e.response.json())
        except ValueError:
            log.error(e.response.text)

        raise


class TaskQueue(object):
    def __init__(self, project, task_queue, creds=None, google_api_lock=None,
                 location=LOCATION):
        # pylint: disable=too-many-arguments
        self.api_root = '{}/projects/{}/locations/{}/queues/{}'.format(
            API_ROOT, project, location, task_queue)

        self.google_api_lock = google_api_lock or threading.RLock()

        self.access_token = Token(creds=creds,
                                  google_api_lock=self.google_api_lock,
                                  scopes=SCOPES)

        self.default_header = {
            'Accept': 'application/json',
            'Content-Length': '0',
        }

    def headers(self):
        header = {k: v for k, v in self.default_header.items()}
        header.update({
            'Authorization': 'Bearer {}'.format(self.access_token)
        })

        return header

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/acknowledge
    def ack(self, task):
        url = '{}/{}:acknowledge'.format(API_ROOT, task['name'])

        body = {
            'scheduleTime': task['scheduleTime'],
        }

        with self.google_api_lock:
            resp = requests.post(url, headers=self.headers(), json=body)

        raise_for_status(resp)
        return resp.json()

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/cancelLease
    def cancel(self, task):
        url = '{}/{}:cancelLease'.format(API_ROOT, task['name'])

        body = {
            'scheduleTime': task['scheduleTime'],
            'responseView': 'BASIC',
        }

        with self.google_api_lock:
            resp = requests.post(url, headers=self.headers(), json=body)

        raise_for_status(resp)
        return resp.json()

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/delete
    def delete(self, tname):
        url = '{}/{}'.format(API_ROOT, tname)

        with self.google_api_lock:
            resp = requests.delete(url, headers=self.headers())

        raise_for_status(resp)
        return resp.json()

    def drain(self):
        tasks = self.list(page_size=1000)
        while tasks:
            for task in tasks['tasks']:
                try:
                    self.delete(task['name'])
                except requests.exceptions.HTTPError:
                    pass

            tasks = self.list(page_size=1000,
                              page_token=tasks['nextPageToken'])

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/get
    def get(self, tname, full=False):
        url = '{}/{}'.format(API_ROOT, tname)
        params = {
            'responseView': 'FULL' if full else 'BASIC',
        }

        with self.google_api_lock:
            resp = requests.get(url, headers=self.headers(), params=params)

        raise_for_status(resp)
        return resp.json()

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/create
    def insert(self, payload, tag=None):
        url = '{}/tasks'.format(self.api_root)

        body = {
            'task': {
                'pullMessage': {
                    'payload': payload,
                    'tag': tag,
                },
            },
            'responseView': 'FULL',
        }

        with self.google_api_lock:
            resp = requests.post(url, headers=self.headers(), json=body)

        raise_for_status(resp)
        return resp.json()

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/lease
    def lease(self, num_tasks=1, lease_duration=10, task_filter=None):
        url = '{}/tasks:lease'.format(self.api_root)

        body = {
            'maxTasks': min(num_tasks, 1000),
            'leaseDuration': '{}s'.format(lease_duration),
            'responseView': 'FULL',
        }
        if task_filter:
            body['filter'] = task_filter

        with self.google_api_lock:
            resp = requests.post(url, headers=self.headers(), json=body)

        raise_for_status(resp)
        return resp.json()

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/list
    def list(self, full=False, page_size=1000, page_token=''):
        url = '{}/tasks'.format(self.api_root)
        params = {
            'responseView': 'FULL' if full else 'BASIC',
            'pageSize': page_size,
            'pageToken': page_token,
        }

        with self.google_api_lock:
            resp = requests.get(url, headers=self.headers(), params=params)

        raise_for_status(resp)
        return resp.json()

    # https://cloud.google.com/cloud-tasks/docs/reference/rest/v2beta2/projects.locations.queues.tasks/renewLease
    def renew(self, task, lease_duration=10):
        url = '{}/{}:renewLease'.format(API_ROOT, task['name'])

        body = {
            'scheduleTime': task['scheduleTime'],
            'leaseDuration': '{}s'.format(lease_duration),
            'responseView': 'FULL',
        }

        with self.google_api_lock:
            resp = requests.post(url, headers=self.headers(), json=body)

        raise_for_status(resp)
        return resp.json()
