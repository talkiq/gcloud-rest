import time

import requests

from gcloud.rest.datastore import Datastore
from gcloud.rest.datastore import Filter
from gcloud.rest.datastore import GQLQuery
from gcloud.rest.datastore import Key
from gcloud.rest.datastore import Operation
from gcloud.rest.datastore import PathElement
from gcloud.rest.datastore import PropertyFilter
from gcloud.rest.datastore import PropertyFilterOperator
from gcloud.rest.datastore import Query
from gcloud.rest.datastore import Value


def test_query(creds, kind, project):
    # type: (str, str, str) -> None
    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)

        property_filter = PropertyFilter(
            prop='value', operator=PropertyFilterOperator.EQUAL,
            value=Value(42))
        query = Query(kind=kind, query_filter=Filter(property_filter))

        before = ds.runQuery(query, session=s)
        num_results = len(before.entity_results)

        transaction = ds.beginTransaction(session=s)
        mutations = [
            ds.make_mutation(Operation.INSERT,
                             Key(project, [PathElement(kind)]),
                             properties={'value': 42}),
            ds.make_mutation(Operation.INSERT,
                             Key(project, [PathElement(kind)]),
                             properties={'value': 42}),
        ]
        ds.commit(mutations, transaction=transaction, session=s)

        # TODO: figure out why this is flaky without the sleep
        # Seems to only be flaky intermittently in py2.
        time.sleep(2)

        after = ds.runQuery(query, session=s)
        assert len(after.entity_results) == num_results + 2


def test_gql_query(creds, kind, project):
    # type: (str, str, str) -> None
    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)

        query = GQLQuery('SELECT * FROM %s WHERE value = @value' % kind,
                         named_bindings={'value': 42})

        before = ds.runQuery(query, session=s)
        num_results = len(before.entity_results)

        transaction = ds.beginTransaction(session=s)
        mutations = [
            ds.make_mutation(Operation.INSERT,
                             Key(project, [PathElement(kind)]),
                             properties={'value': 42}),
            ds.make_mutation(Operation.INSERT,
                             Key(project, [PathElement(kind)]),
                             properties={'value': 42}),
            ds.make_mutation(Operation.INSERT,
                             Key(project, [PathElement(kind)]),
                             properties={'value': 42}),
        ]
        ds.commit(mutations, transaction=transaction, session=s)

        # TODO: figure out why this is flaky without the sleep
        # Seems to only be flaky intermittently in py2.
        time.sleep(2)

        after = ds.runQuery(query, session=s)
        assert len(after.entity_results) == num_results + 3
