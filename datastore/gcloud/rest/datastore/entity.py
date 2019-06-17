from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import
from typing import Optional  # pylint: disable=unused-import

from gcloud.rest.datastore.key import Key
from gcloud.rest.datastore.value import Value


class Entity(object):
    key_kind = Key
    value_kind = Value

    def __init__(self, key, properties=None):
        # type: (Key, Optional[Dict[str, dict]]) -> None
        self.key = key
        self.properties = {k: self.value_kind.from_repr(v).value
                           for k, v in (properties or {}).items()}

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, Entity):
            return False

        return bool(self.key == other.key
                    and self.properties == other.properties)

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> Entity
        return cls(cls.key_kind.from_repr(data['key']), data.get('properties'))

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'key': self.key.to_repr(),
            'properties': self.properties,
        }


class EntityResult(object):
    entity_kind = Entity

    def __init__(self, entity, version, cursor=''):
        # type: (Entity, str, str) -> None
        self.entity = entity
        self.version = version
        self.cursor = cursor

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, EntityResult):
            return False

        return bool(self.entity == other.entity
                    and self.version == other.version
                    and self.cursor == self.cursor)

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> EntityResult
        return cls(cls.entity_kind.from_repr(data['entity']), data['version'],
                   data.get('cursor', ''))

    def to_repr(self):
        # type: () -> Dict[str, Any]
        data = {
            'entity': self.entity.to_repr(),
            'version': self.version,
        }
        if self.cursor:
            data['cursor'] = self.cursor

        return data
