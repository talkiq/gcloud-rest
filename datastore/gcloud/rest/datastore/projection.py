from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import

# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#Projection


class Projection(object):
    def __init__(self, prop):
        # type: (str) -> None
        self.prop = prop

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, Projection):
            return False

        return bool(self.prop == other.prop)

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> Projection
        return cls(prop=data['property']['name'])

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'property': {'name': self.prop},
        }
