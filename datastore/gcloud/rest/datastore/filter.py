from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import
from typing import List  # pylint: disable=unused-import

from gcloud.rest.datastore.constants import CompositeFilterOperator
from gcloud.rest.datastore.constants import PropertyFilterOperator
from gcloud.rest.datastore.value import Value


class BaseFilter(object):
    json_key = ''

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> BaseFilter
        raise NotImplementedError

    def to_repr(self):
        # type: () -> Dict[str, Any]
        raise NotImplementedError


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#Filter
class Filter(object):
    def __init__(self, inner_filter):
        # type: (BaseFilter) -> None
        self.inner_filter = inner_filter

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, Filter):
            return False

        return self.inner_filter == other.inner_filter

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> Filter
        if 'compositeFilter' in data:
            return cls(CompositeFilter.from_repr(data['compositeFilter']))
        if 'propertyFilter' in data:
            return cls(PropertyFilter.from_repr(data['propertyFilter']))

        raise ValueError('invalid filter name: %s' % data.keys())

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            self.inner_filter.json_key: self.inner_filter.to_repr(),
        }


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#CompositeFilter
class CompositeFilter(BaseFilter):
    json_key = 'compositeFilter'

    def __init__(self, operator, filters):
        # type: (CompositeFilterOperator, List[Filter]) -> None
        self.operator = operator
        self.filters = filters

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, CompositeFilter):
            return False

        return bool(
            self.operator == other.operator
            and self.filters == other.filters)

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> CompositeFilter
        operator = CompositeFilterOperator(data['op'])
        filters = [Filter.from_repr(f) for f in data['filters']]
        return cls(operator=operator, filters=filters)

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'filters': [f.to_repr() for f in self.filters],
            'op': self.operator.value,
        }


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#PropertyFilter
class PropertyFilter(BaseFilter):
    json_key = 'propertyFilter'

    def __init__(self, prop, operator, value):
        # type: (str, PropertyFilterOperator, Value) -> None
        self.prop = prop
        self.operator = operator
        self.value = value

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, PropertyFilter):
            return False

        return bool(
            self.prop == other.prop
            and self.operator == other.operator
            and self.value == other.value)

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> PropertyFilter
        prop = data['property']['name']
        operator = PropertyFilterOperator(data['op'])
        value = Value.from_repr(data['value'])
        return cls(prop=prop, operator=operator, value=value)

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'op': self.operator.value,
            'property': {'name': self.prop},
            'value': self.value.to_repr(),
        }
