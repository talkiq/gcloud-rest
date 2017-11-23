import logging

import requests

from gcloud.rest.auth import Token


log = logging.getLogger(__name__)

API_ROOT = 'https://www.googleapis.com/taskqueue/v1beta2/projects'
SCOPES = [
    'https://www.googleapis.com/auth/cloud-taskqueue',
    'https://www.googleapis.com/auth/cloud-taskqueue.consumer',
    'https://www.googleapis.com/auth/taskqueue',
    'https://www.googleapis.com/auth/taskqueue.consumer',
]


class TaskQueue(object):
    def __init__(self, project, task_queue, creds=None):
        self.project = project
        self.task_queue = task_queue

        self.access_token = Token(creds=creds, scopes=SCOPES)

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

    def delete(self, tid):
        url = '{}/s~{}/taskqueues/{}/tasks/{}'.format(
            API_ROOT, self.project, self.task_queue, tid)

        log.debug('deleting task %s', tid)
        resp = requests.delete(url, headers=self.headers())
        resp.raise_for_status()
