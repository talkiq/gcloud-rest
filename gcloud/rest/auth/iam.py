import json
import threading
from typing import Dict  # pylint: disable=unused-import
from typing import List  # pylint: disable=unused-import
from typing import Optional  # pylint: disable=unused-import
from typing import Union  # pylint: disable=unused-import

import requests

from .token import Token
from .token import Type
from .utils import encode


API_ROOT_IAM = 'https://iam.googleapis.com/v1'
API_ROOT_IAM_CREDENTIALS = 'https://iamcredentials.googleapis.com/v1'
SCOPES = ['https://www.googleapis.com/auth/iam']


class IamClient(object):
    def __init__(self,
                 service_file=None,  # type: Optional[str]
                 session=None,  # type: Optional[requests.Session]
                 google_api_lock=None,  # type: Optional[threading.RLock]
                 token=None  # type: Optional[Token]
                 ):
        # type: (...) -> None
        self.session = session
        self.google_api_lock = google_api_lock or threading.RLock()
        self.token = token or Token(service_file=service_file,
                                    session=session, scopes=SCOPES)

        if self.token.token_type != Type.SERVICE_ACCOUNT:
            raise TypeError('IAM Credentials Client is only valid for use'
                            ' with Service Accounts')

    def headers(self):
        # type: () -> Dict[str, str]
        token = self.token.get()
        return {
            'Authorization': 'Bearer {}'.format(token),
        }

    @property
    def service_account_email(self):
        # type: () -> Optional[str]
        return self.token.service_data.get('client_email')

    # https://cloud.google.com/iam/reference/rest/v1/projects.serviceAccounts.keys/get
    def get_public_key(self,
                       key_id=None,                 # type: Optional[str]
                       key=None,                    # type: Optional[str]
                       service_account_email=None,  # type: Optional[str]
                       project=None,                # type: Optional[str]
                       session=None,                # type: requests.Session
                       timeout=10                   # type: int
                       ):
        # type: (...) -> Dict[str, str]
        service_account_email = (service_account_email
                                 or self.service_account_email)
        project = project or self.token.get_project()

        if not key_id and not key:
            raise ValueError('get_public_key must have either key_id or key')

        if not key:
            key = 'projects/{}/serviceAccounts/{}/keys/{}' \
                .format(project, service_account_email, key_id)

        url = '{}/{}?publicKeyType=TYPE_X509_PEM_FILE'.format(
            API_ROOT_IAM, key)
        headers = self.headers()

        if not self.session:
            self.session = requests.Session()

        session = session or self.session
        with self.google_api_lock:
            resp = session.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    # https://cloud.google.com/iam/reference/rest/v1/projects.serviceAccounts.keys/list
    def list_public_keys(self,
                         service_account_email=None,  # type: Optional[str]
                         project=None,  # type: Optional[str]
                         session=None,  # type: requests.Session
                         timeout=10  # type: int
                         ):
        # type: (...) -> List[Dict[str, str]]
        service_account_email = (service_account_email
                                 or self.service_account_email)
        project = project or self.token.get_project()

        url = ('{}/projects/{}/serviceAccounts/'
               '{}/keys').format(API_ROOT_IAM, project, service_account_email)

        headers = self.headers()

        if not self.session:
            self.session = requests.Session()

        session = session or self.session
        with self.google_api_lock:
            resp = session.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json().get('keys', [])

    # https://cloud.google.com/iam/credentials/reference/rest/v1/projects.serviceAccounts/signBlob
    def sign_blob(self,
                  payload,  # type: Optional[Union[str, bytes]]
                  service_account_email=None,  # type: Optional[str]
                  delegates=None,              # type: Optional[list]
                  session=None,                # type: requests.Session
                  timeout=10                   # type: int
                  ):
        # type: (...) -> Dict[str, str]
        service_account_email = (
            service_account_email or self.service_account_email)
        if not service_account_email:
            raise TypeError('sign_blob must have a valid '
                            'service_account_email')

        resource_name = 'projects/-/serviceAccounts/{}'.format(
            service_account_email)
        url = '{}/{}:signBlob'.format(API_ROOT_IAM_CREDENTIALS, resource_name)

        json_str = json.dumps({
            'delegates': delegates or [resource_name],
            'payload': encode(payload).decode('utf-8'),
        })

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(json_str)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()

        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=json_str,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
