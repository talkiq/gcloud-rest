import uuid

import requests

from gcloud.rest.datastore import Datastore
from gcloud.rest.datastore import Key
from gcloud.rest.datastore import Operation
from gcloud.rest.datastore import PathElement


def test_item_lifecycle(creds, kind, project):
    # type: (str, str, str) -> None
    key = Key(project, [PathElement(kind)])

    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)

        allocatedKeys = ds.allocateIds([key], session=s)
        assert len(allocatedKeys) == 1
        key.path[-1].id = allocatedKeys[0].path[-1].id
        assert key == allocatedKeys[0]

        ds.reserveIds(allocatedKeys, session=s)

        props_insert = {'is_this_bad_data': True}
        ds.insert(allocatedKeys[0], props_insert, session=s)
        actual = ds.lookup([allocatedKeys[0]], session=s)
        assert actual['found'][0].entity.properties == props_insert

        props_update = {'animal': 'aardvark', 'overwrote_bad_data': True}
        ds.update(allocatedKeys[0], props_update, session=s)
        actual = ds.lookup([allocatedKeys[0]], session=s)
        assert actual['found'][0].entity.properties == props_update

        props_upsert = {'meaning_of_life': 42}
        ds.upsert(allocatedKeys[0], props_upsert, session=s)
        actual = ds.lookup([allocatedKeys[0]], session=s)
        assert actual['found'][0].entity.properties == props_upsert

        ds.delete(allocatedKeys[0], session=s)
        actual = ds.lookup([allocatedKeys[0]], session=s)
        assert len(actual['missing']) == 1


def test_transaction(creds, kind, project):
    # type: (str, str, str) -> None
    path_element_name = 'test_record_%s' % uuid.uuid4()
    key = Key(project, [PathElement(kind, name=path_element_name)])

    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)

        transaction = ds.beginTransaction(session=s)
        actual = ds.lookup([key], transaction=transaction, session=s)
        assert len(actual['missing']) == 1

        mutations = [
            ds.make_mutation(Operation.INSERT, key,
                             properties={'animal': 'three-toed sloth'}),
            ds.make_mutation(Operation.UPDATE, key,
                             properties={'animal': 'aardvark'}),
        ]
        ds.commit(mutations, transaction=transaction, session=s)

        actual = ds.lookup([key], session=s)
        assert actual['found'][0].entity.properties == {'animal': 'aardvark'}


def test_rollback(creds, project):
    # type: (str, str) -> None
    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)

        transaction = ds.beginTransaction(session=s)
        ds.rollback(transaction, session=s)
