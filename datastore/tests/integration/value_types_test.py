"""Make sure all value types are serialized/deserialized correctly"""
import requests

from gcloud.rest.datastore import Datastore
from gcloud.rest.datastore import Key
from gcloud.rest.datastore import LatLng
from gcloud.rest.datastore import PathElement


def test_geo_point_value(creds, kind, project):
    # type: (str, str, str) -> None
    key = Key(project, [PathElement(kind)])

    with requests.Session() as s:
        ds = Datastore(project=project, service_file=creds, session=s)

        allocatedKeys = ds.allocateIds([key], session=s)
        ds.reserveIds(allocatedKeys, session=s)

        props_insert = {'location': LatLng(49.2827, 123.1207)}
        ds.insert(allocatedKeys[0], props_insert, session=s)
        actual = ds.lookup([allocatedKeys[0]], session=s)
        assert actual['found'][0].entity.properties == props_insert
