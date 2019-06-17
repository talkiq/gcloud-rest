from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import

from gcloud.rest.datastore.constants import Direction


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#PropertyOrder
class PropertyOrder(object):
    def __init__(self, prop, direction=Direction.ASCENDING):
        # type: (str, Direction) -> None
        self.prop = prop
        self.direction = direction

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, PropertyOrder):
            return False

        return bool(
            self.prop == other.prop
            and self.direction == other.direction)

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> PropertyOrder
        prop = data['property']['name']
        direction = Direction(data['direction'])
        return cls(prop=prop, direction=direction)

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'property': {'name': self.prop},
            'direction': self.direction.value,
        }
