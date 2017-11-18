import datetime
import logging
import os

import requests
from oauth2client.service_account import ServiceAccountCredentials


TIMEOUT = 60

log = logging.getLogger(__name__)


class Token(object):
    def __init__(self, creds=None, scopes=None, timeout=TIMEOUT):
        self.creds = creds or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not self.creds:
            raise Exception('could not load service credentials')
        self.scopes = scopes or []
        self.timeout = timeout

        self.age = datetime.datetime.now()
        self.expiry = 60
        self.value = None

    def __str__(self):
        self.ensure()
        return str(self.value)

    def acquire(self):
        """
        acquires a new token
        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.creds, self.scopes)

        url = credentials.token_uri
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        m = b'grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion='  # pylint: disable=line-too-long
        # TODO: do not rely on private method -- extract out the relevant chunk
        # for ourselves
        assertion = credentials._generate_assertion()  # pylint: disable=protected-access
        body = m + assertion

        response = requests.post(url, data=body, headers=headers,
                                 timeout=self.timeout)
        content = response.json()

        if 'error' in content:
            raise Exception('{}'.format(content))

        self.age = datetime.datetime.now()
        self.expiry = int(content['expires_in'])
        self.value = content['access_token']

    def ensure(self):
        if not self.value:
            log.debug('acquiring initial token')
            self.acquire()
            return

        now = datetime.datetime.now()
        delta = (now - self.age).total_seconds()

        if delta > self.expiry / 2:
            log.debug('requiring token with expiry %d of %d / 2', delta,
                      self.expiry)
            self.acquire()
