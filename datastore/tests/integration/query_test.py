import time

import requests

from gcloud.rest.datastore import Datastore
from gcloud.rest.datastore import Filter
from gcloud.rest.datastore import GQLQuery
from gcloud.rest.datastore import Key
from gcloud.rest.datastore import Operation
from gcloud.rest.datastore import PathElement
from gcloud.rest.datastore import Projection
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


def test_query_with_value_projection(creds, kind, project):
    # type: (str, str, str) -> None
    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)
        # setup test data
        ds.insert(Key(project, [PathElement(kind)]), {'value': 30}, s)
        projection = [Projection.from_repr({'property': {'name': 'value'}})]

        query = Query(kind=kind, limit=1,
                      projection=projection)
        result = ds.runQuery(query, session=s)
        assert result.entity_result_type.value == 'PROJECTION'
        # clean up test data
        ds.delete(result.entity_results[0].entity.key, s)


def test_query_with_key_projection(creds, kind, project):
    # type: (str, str, str) -> None
    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)
        # setup test data
        ds.insert(Key(project, [PathElement(kind)]), {'value': 30}, s)
        property_filter = PropertyFilter(
            prop='value', operator=PropertyFilterOperator.EQUAL,
            value=Value(30))
        projection = [Projection.from_repr({'property': {'name': '__key__'}})]

        query = Query(kind=kind, query_filter=Filter(property_filter), limit=1,
                      projection=projection)
        result = ds.runQuery(query, session=s)
        assert result.entity_results[0].entity.properties == {}
        assert result.entity_result_type.value == 'KEY_ONLY'
        # clean up test data
        ds.delete(result.entity_results[0].entity.key, s)


def test_query_with_distinct_on(creds, kind, project):
    # type: (str, str, str) -> None
    keys1 = [Key(project, [PathElement(kind)])
             for i in range(3)]  # pylint: disable=unused-variable
    keys2 = [Key(project, [PathElement(kind)])
             for i in range(3)]  # pylint: disable=unused-variable
    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)

        # setup test data
        allocatedKeys1 = ds.allocateIds(keys1, session=s)
        allocatedKeys2 = ds.allocateIds(keys2, session=s)
        for key1 in allocatedKeys1:
            ds.insert(key1, {'dist_value': 11}, s)
        for key2 in allocatedKeys2:
            ds.insert(key2, {'dist_value': 22}, s)
        query = Query(kind=kind, limit=10, distinct_on=['dist_value'])
        result = ds.runQuery(query, session=s)
        assert len(result.entity_results) == 2
        # clean up test data
        for key1 in allocatedKeys1:
            ds.delete(key1, s)
        for key2 in allocatedKeys2:
            ds.delete(key2, s)
