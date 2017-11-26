import logging
import threading

import requests

from gcloud.rest.auth import Token


log = logging.getLogger(__name__)

API_ROOT = 'https://www.googleapis.com/storage/v1/b'
# TODO: storage upload
# UPLOAD_API_ROOT = 'https://www.googleapis.com/upload/storage/v1/b'
SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_only',
]


class Bucket(object):
    def __init__(self, project, bucket, creds=None, google_api_lock=None):
        self.project = project
        self.bucket = bucket

        self.google_api_lock = google_api_lock or threading.RLock()

        self.access_token = Token(creds=creds,
                                  google_api_lock=self.google_api_lock,
                                  scopes=SCOPES)

    def headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.access_token)
        }

    def download(self, object_name, params=None):
        # TODO: does this need actual urlescaping?
        object_name = object_name.replace('/', '%2F')
        url = '{}/{}/o/{}'.format(API_ROOT, self.bucket, object_name)

        with self.google_api_lock:
            resp = requests.get(url, headers=self.headers(), params=params or {})

        resp.raise_for_status()
        return resp.text

    def download_as_string(self, object_name):
        return self.download(object_name, params={'alt': 'media'})
