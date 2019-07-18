from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/LatLng
class LatLng(object):
    def __init__(self, lat, lon):
        # type: (str, float, float) -> None
        self.lat = lat
        self.lon = lon

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, LatLng):
            return False

        return bool(
            self.lat == other.lat
            and self.lon == other.lon)

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> LatLng
        lat = data['latitude']
        lon = data['longitude']
        return cls(lat=lat, lon=lon)

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'latitude': self.lat,
            'longitude': self.lon,
        }
