import threading

import requests

from gcloud.rest.auth import Token


API_ROOT = 'https://cloudkms.googleapis.com/v1'
LOCATION = 'global'
SCOPES = [
    'https://www.googleapis.com/auth/cloudkms',
]


class KMS(object):
    def __init__(self, project, keyring, keyname, creds=None,
                 google_api_lock=None, location=LOCATION):
        # pylint: disable=too-many-arguments
        self.api_root = ('{}/projects/{}/locations/{}/keyRings/{}/'
                         'cryptoKeys/{}'.format(API_ROOT, project, location,
                                                keyring, keyname))

        self.google_api_lock = google_api_lock or threading.RLock()

        self.access_token = Token(service_file=creds,
                                  google_api_lock=self.google_api_lock,
                                  scopes=SCOPES)

    def headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.access_token),
        }

    # https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/decrypt
    def decrypt(self, ciphertext):
        url = '{}:decrypt'.format(self.api_root)
        body = {'ciphertext': ciphertext}

        with self.google_api_lock:
            resp = requests.post(url, headers=self.headers(), json=body)

        resp.raise_for_status()
        return resp.json()['plaintext']

    # https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt
    def encrypt(self, plaintext):
        url = '{}:encrypt'.format(self.api_root)
        body = {'plaintext': plaintext}

        with self.google_api_lock:
            resp = requests.post(url, headers=self.headers(), json=body)

        resp.raise_for_status()
        return resp.json()['ciphertext']
