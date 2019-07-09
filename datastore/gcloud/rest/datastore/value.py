from datetime import datetime
from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import

from gcloud.rest.datastore.constants import TypeName
from gcloud.rest.datastore.constants import TYPES
from gcloud.rest.datastore.key import Key


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#value
class Value(object):
    key_kind = Key

    def __init__(self, value, exclude_from_indexes=False):
        # type: (Any, bool) -> None
        self.value = value
        self.excludeFromIndexes = exclude_from_indexes

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, Value):
            return False

        return bool(
            self.excludeFromIndexes == other.excludeFromIndexes
            and self.value == other.value)

    def __repr__(self):
        # type () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> Value
        supported_types = cls._get_supported_types()
        for value_type, type_name in supported_types.items():
            json_key = type_name.value
            if json_key in data:
                if json_key == 'nullValue':
                    value = None
                elif value_type == datetime:
                    value = datetime.strptime(data[json_key],
                                              '%Y-%m-%dT%H:%M:%S.%f000Z')
                elif value_type == cls.key_kind:
                    value = cls.key_kind.from_repr(data[json_key])
                else:
                    value = value_type(data[json_key])
                break
        else:
            supported = [name.value for name in supported_types.values()]
            raise NotImplementedError(
                '%s does not contain a supported value type'
                ' (any of: %s)' % (data.keys(), supported))

        # Google may not populate that field. This can happen with both
        # indexed and non-indexed fields.
        exclude_from_indexes = bool(data.get('excludeFromIndexes', False))

        return cls(value=value, exclude_from_indexes=exclude_from_indexes)

    def to_repr(self):
        # type: () -> Dict[str, Any]
        value_type = self._infer_type(self.value)
        if value_type == TypeName.KEY:
            value = self.value.to_repr()
        elif value_type == TypeName.TIMESTAMP:
            value = self.value.strftime('%Y-%m-%dT%H:%M:%S.%f000Z')
        else:
            value = 'NULL_VALUE' if self.value is None else self.value
        return {
            'excludeFromIndexes': self.excludeFromIndexes,
            value_type.value: value,
        }

    def _infer_type(self, value):
        # type: (Any) -> TypeName
        kind = type(value)
        supported_types = self._get_supported_types()

        try:
            return supported_types[kind]
        except KeyError:
            raise NotImplementedError(
                '%s is not a supported value type'
                ' (any of: %s)' % (kind, supported_types))

    @classmethod
    def _get_supported_types(cls):
        # type: () -> Dict[type, TypeName]
        supported_types = TYPES
        supported_types.update({
            cls.key_kind: TypeName.KEY,
        })
        return supported_types
