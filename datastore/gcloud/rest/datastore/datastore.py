import logging
import os
import threading
from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import
from typing import List  # pylint: disable=unused-import
from typing import Optional  # pylint: disable=unused-import
from typing import Union  # pylint: disable=unused-import

import requests

from gcloud.rest.auth import Token  # pylint: disable=no-name-in-module
from gcloud.rest.datastore.constants import Consistency
from gcloud.rest.datastore.constants import Mode
from gcloud.rest.datastore.constants import Operation
from gcloud.rest.datastore.datastore_operation import DatastoreOperation
from gcloud.rest.datastore.entity import EntityResult
from gcloud.rest.datastore.key import Key
from gcloud.rest.datastore.query import BaseQuery  # pylint: disable=unused-import
from gcloud.rest.datastore.query import QueryResultBatch
from gcloud.rest.datastore.value import Value
try:
    import ujson as json
except ImportError:
    import json  # type: ignore


try:
    API_ROOT = 'http://%s/v1' % os.environ['DATASTORE_EMULATOR_HOST']
    IS_DEV = True
except KeyError:
    API_ROOT = 'https://datastore.googleapis.com/v1'
    IS_DEV = False

SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/datastore',
]

log = logging.getLogger(__name__)


class Datastore(object):
    datastore_operation_kind = DatastoreOperation
    entity_result_kind = EntityResult
    key_kind = Key
    query_result_batch_kind = QueryResultBatch
    value_kind = Value

    def __init__(self,
                 project=None,          # type: Optional[str]
                 service_file=None,     # type: Optional[str]
                 namespace='',          # type: str
                 session=None,          # type: Optional[requests.Session]
                 token=None,            # type: Optional[Token]
                 google_api_lock=None,  # type: Optional[threading.RLock]
                 ):
        # type: (...) -> None
        self.namespace = namespace
        self.session = session
        self.google_api_lock = google_api_lock or threading.RLock()

        if IS_DEV:
            self._project = os.environ.get('DATASTORE_PROJECT_ID', 'dev')
            # Tokens are not needed when using dev emulator
            self.token = None
        else:
            self._project = project
            self.token = token or Token(service_file=service_file,
                                        session=session, scopes=SCOPES)

    def project(self):
        # type: () -> str
        if self._project:
            return self._project

        self._project = self.token.get_project()
        if self._project:
            return self._project

        raise Exception('could not determine project, please set it manually')

    @staticmethod
    def _make_commit_body(mutations, transaction=None,
                          mode=Mode.TRANSACTIONAL):
        # type: (List[Dict[str, Any]], Optional[str], Mode) -> Dict[str, Any]
        if not mutations:
            raise Exception('at least one mutation record is required')

        if transaction is None and mode != Mode.NON_TRANSACTIONAL:
            raise Exception('a transaction ID must be provided when mode is '
                            'transactional')

        data = {
            'mode': mode.value,
            'mutations': mutations,
        }
        if transaction is not None:
            data['transaction'] = transaction
        return data

    def headers(self):
        # type: () -> Dict[str, str]
        if IS_DEV:
            return {}

        token = self.token.get()
        return {
            'Authorization': 'Bearer %s' % token,
        }

    # TODO: support mutations w version specifiers, return new version (commit)
    @classmethod
    def make_mutation(cls, operation, key, properties=None):
        # type: (Operation, Key, Optional[Dict[str, Any]]) -> Dict[str, Any]
        if operation == Operation.DELETE:
            return {operation.value: key.to_repr()}

        return {
            operation.value: {
                'key': key.to_repr(),
                'properties': {k: cls.value_kind(v).to_repr()
                               for k, v in properties.items()},
            }
        }

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/allocateIds
    def allocateIds(self, keys, session=None, timeout=10):
        # type: (List[Key], Optional[requests.Session], int) -> List[Key]

        project = self.project()
        url = '%s/projects/%s:allocateIds' % (API_ROOT, project)

        payload = json.dumps({
            'keys': [k.to_repr() for k in keys],
        }).encode('utf-8')

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=payload,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()  # type: dict

        return [self.key_kind.from_repr(k) for k in data['keys']]

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/beginTransaction
    # TODO: support readwrite vs readonly transaction types
    def beginTransaction(self, session=None, timeout=10):
        # type: (requests.Session, int) -> str
        project = self.project()
        url = '%s/projects/%s:beginTransaction' % (API_ROOT, project)
        headers = self.headers()
        headers.update({
            'Content-Length': '0',
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        transaction = data['transaction']  # type: str
        return transaction

    # TODO: return mutation results
    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit
    def commit(self,
               mutations,                # type: List[Dict[str, Any]]
               transaction=None,         # type: Optional[str]
               mode=Mode.TRANSACTIONAL,  # type: Mode
               session=None,             # type: Optional[requests.Session]
               timeout=1                 # type: int
               ):
        # type: (...) -> None
        project = self.project()
        url = '%s/projects/%s:commit' % (API_ROOT, project)

        body = self._make_commit_body(mutations, transaction=transaction,
                                      mode=mode)
        payload = json.dumps(body).encode('utf-8')

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=payload,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()

    # https://cloud.google.com/datastore/docs/reference/admin/rest/v1/projects/export
    def export(self,
               output_bucket_prefix,  # type: str
               kinds=None,            # type: Optional[List[str]]
               namespaces=None,       # type: Optional[List[str]]
               labels=None,           # type: Optional[Dict[str, str]]
               session=None,          # type: Optional[requests.Session]
               timeout=10             # type: int
               ):
        # type: (...) -> DatastoreOperation
        project = self.project()
        url = '%s/projects/%s:export' % (API_ROOT, project)

        payload = json.dumps({
            'entityFilter': {
                'kinds': kinds or [],
                'namespaceIds': namespaces or [],
            },
            'labels': labels or {},
            'outputUrlPrefix': 'gs://' + output_bucket_prefix,
        }).encode('utf-8')

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=payload,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()  # type: dict

        return self.datastore_operation_kind.from_repr(data)

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/get
    def get_datastore_operation(self, name, session, timeout):
        # type: (str, requests.Session, int) -> DatastoreOperation
        url = '%s/%s' % (API_ROOT, name)

        headers = self.headers()
        headers.update({
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()  # type: dict

        return self.datastore_operation_kind.from_repr(data)

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/lookup
    def lookup(self,
               keys,                            # type: List[Key]
               transaction=None,                # type: Optional[str]
               consistency=Consistency.STRONG,  # type: Consistency
               session=None,                 # type: Optional[requests.Session]
               timeout=10                       # type: int
               ):
        # type: (...) -> Dict[str, Union[EntityResult, Key]]
        project = self.project()
        url = '%s/projects/%s:lookup' % (API_ROOT, project)

        if transaction:
            options = {'transaction': transaction}
        else:
            options = {'readConsistency': consistency.value}
        payload = json.dumps({
            'keys': [k.to_repr() for k in keys],
            'readOptions': options,
        }).encode('utf-8')

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=payload,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()  # type: dict

        return {
            'found': [self.entity_result_kind.from_repr(e)
                      for e in data.get('found', [])],
            'missing': [self.entity_result_kind.from_repr(e)
                        for e in data.get('missing', [])],
            'deferred': [self.key_kind.from_repr(k)
                         for k in data.get('deferred', [])],
        }

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/reserveIds
    def reserveIds(self, keys, database_id='', session=None, timeout=10):
        # type (List[Key], str, Optional[requests.Session], int) -> None
        project = self.project()
        url = '%s/projects/%s:reserveIds' % (API_ROOT, project)

        payload = json.dumps({
            'databaseId': database_id,
            'keys': [k.to_repr() for k in keys],
        }).encode('utf-8')

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=payload,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/rollback
    def rollback(self, transaction, session=None, timeout=10):
        # type: (str, requests.Session, int) -> None
        project = self.project()
        url = '%s/projects/%s:rollback' % (API_ROOT, project)

        payload = json.dumps({
            'transaction': transaction,
        }).encode('utf-8')

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=payload,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()

    # https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery
    def runQuery(self,
                 query,                             # type: BaseQuery
                 transaction=None,                  # type: Optional[str]
                 consistency=Consistency.EVENTUAL,  # type: Consistency
                 session=None,               # type: Optional[requests.Session]
                 timeout=10                         # type: int
                 ):
        # type (...) -> QueryResultBatch
        project = self.project()
        url = '%s/projects/%s:runQuery' % (API_ROOT, project)

        if transaction:
            options = {'transaction': transaction}
        else:
            options = {'readConsistency': consistency.value}
        payload = json.dumps({
            'partitionId': {
                'projectId': project,
                'namespaceId': self.namespace,
            },
            query.json_key: query.to_repr(),
            'readOptions': options,
        }).encode('utf-8')

        headers = self.headers()
        headers.update({
            'Content-Length': str(len(payload)),
            'Content-Type': 'application/json',
        })

        if not self.session:
            self.session = requests.Session()
        session = session or self.session
        with self.google_api_lock:
            resp = session.post(url, data=payload,
                                headers=headers, timeout=timeout)
        resp.raise_for_status()

        data = resp.json()  # type: dict
        return self.query_result_batch_kind.from_repr(data['batch'])

    def delete(self, key, session=None):
        # type: (Key, Optional[requests.Session]) -> None
        return self.operate(Operation.DELETE, key, session=session)

    def insert(self, key, properties, session=None):
        # type: (Key, Dict[str, Any], Optional[requests.Session]) -> None
        return self.operate(Operation.INSERT, key, properties, session=session)

    def update(self, key, properties, session=None):
        # type: (Key, Dict[str, Any], Optional[requests.Session]) -> None
        return self.operate(Operation.UPDATE, key, properties, session=session)

    def upsert(self, key, properties, session=None):
        # type: (Key, Dict[str, Any], Optional[requests.Session]) -> None
        return self.operate(Operation.UPSERT, key, properties, session=session)

    # TODO: accept Entity rather than key/properties?
    def operate(self,
                operation,        # type: Operation
                key,              # type: Key
                properties=None,  # type: Optional[Dict[str, Any]]
                session=None      # type: Optional[requests.Session]
                ):
        # type (...) -> None
        transaction = self.beginTransaction(session=session)
        mutation = self.make_mutation(operation, key, properties=properties)
        self.commit([mutation], transaction=transaction, session=session)
