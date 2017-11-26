import datetime
import json
import logging
import os
import threading
import time
try:
    # python3
    from urllib.parse import urlencode
except ImportError:
    # python2.7
    from urllib import urlencode

import jwt
import requests


TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'
TIMEOUT = 60

log = logging.getLogger(__name__)


class Token(object):
    def __init__(self, creds=None, google_api_lock=None, scopes=None,
                 timeout=TIMEOUT):
        self.creds = creds or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not self.creds:
            raise Exception('could not load service credentials')
        self.google_api_lock = google_api_lock or threading.RLock()
        self.scopes = scopes or []
        self.timeout = timeout

        self.age = datetime.datetime.now()
        self.expiry = 60
        self.value = None

    def __str__(self):
        self.ensure()
        return str(self.value)

    def assertion(self):
        with open(self.creds, 'r') as f:
            credentials = json.loads(f.read())

        # N.B. the below exists to avoid using this private method:
        #   return ServiceAccountCredentials._generate_assertion()
        now = int(time.time())
        payload = {
            'aud': TOKEN_URI,
            'exp': now + 3600,
            'iat': now,
            'iss': credentials['client_email'],
            'scope': ' '.join(self.scopes),
        }

        return jwt.encode(payload, credentials['private_key'],
                          algorithm='RS256')

    def acquire(self):
        """
        acquires a new token
        """
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = urlencode(
            ('grant_type', 'urn:ietf:params:oauth:grant-type:jwt-bearer'),
            ('assertion', self.assertion())
        )

        with self.google_api_lock:
            response = requests.post(TOKEN_URI, data=body, headers=headers,
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
