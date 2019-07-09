from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import
from typing import List  # pylint: disable=unused-import

import pytest

from gcloud.rest.datastore import CompositeFilter
from gcloud.rest.datastore import CompositeFilterOperator
from gcloud.rest.datastore import Filter
from gcloud.rest.datastore import PropertyFilter
from gcloud.rest.datastore import PropertyFilterOperator
from gcloud.rest.datastore import Value


class TestFilter(object):  # pylint: disable=no-init
    @staticmethod
    def test_property_filter_from_repr(property_filters):
        original_filter = property_filters[0]
        data = {
            'property': {
                'name': original_filter.prop
            },
            'op': original_filter.operator,
            'value': original_filter.value.to_repr()
        }

        output_filter = PropertyFilter.from_repr(data)

        assert output_filter == original_filter

    def test_property_filter_to_repr(self, property_filters):
        property_filter = property_filters[0]
        query_filter = Filter(inner_filter=property_filter)

        r = query_filter.to_repr()

        self._assert_is_correct_prop_dict_for_property_filter(
            r['propertyFilter'], property_filter)

    @staticmethod
    def test_composite_filter_from_repr(property_filters):
        original_filter = CompositeFilter(
            operator=CompositeFilterOperator.AND,
            filters=[
                Filter(property_filters[0]),
                Filter(property_filters[1])
            ])
        data = {
            'op': original_filter.operator,
            'filters': [
                {
                    'propertyFilter': {
                        'property': {
                            'name':
                            original_filter.filters[0].inner_filter.prop
                        },
                        'op': original_filter.filters[0].inner_filter.operator,
                        'value': property_filters[0].value.to_repr()
                    }
                },
                {
                    'propertyFilter': {
                        'property': {
                            'name':
                            original_filter.filters[1].inner_filter.prop
                        },
                        'op': original_filter.filters[1].inner_filter.operator,
                        'value': property_filters[1].value.to_repr()
                    }
                },
            ]
        }

        output_filter = CompositeFilter.from_repr(data)

        assert output_filter == original_filter

    def test_composite_filter_to_repr(self, property_filters):
        composite_filter = CompositeFilter(
            operator=CompositeFilterOperator.AND,
            filters=[
                Filter(property_filters[0]),
                Filter(property_filters[1])
            ])
        query_filter = Filter(composite_filter)

        r = query_filter.to_repr()

        composite_filter_dict = r['compositeFilter']
        assert composite_filter_dict['op'] == 'AND'
        self._assert_is_correct_prop_dict_for_property_filter(
            composite_filter_dict['filters'][0]['propertyFilter'],
            property_filters[0])
        self._assert_is_correct_prop_dict_for_property_filter(
            composite_filter_dict['filters'][1]['propertyFilter'],
            property_filters[1])

    @staticmethod
    def test_filter_from_repr(composite_filter):
        original_filter = Filter(inner_filter=composite_filter)

        data = {
            'compositeFilter': original_filter.inner_filter.to_repr()
        }

        output_filter = Filter.from_repr(data)

        assert output_filter == original_filter

    @staticmethod
    def test_filter_from_repr_unexpected_filter_name():
        unexpected_filter_name = 'unexpectedFilterName'
        data = {
            unexpected_filter_name: 'DoesNotMatter'
        }

        with pytest.raises(ValueError) as ex_info:
            Filter.from_repr(data)

        assert unexpected_filter_name in ex_info.value.args[0]

    @staticmethod
    def test_filter_to_repr(composite_filter):
        test_filter = Filter(inner_filter=composite_filter)

        r = test_filter.to_repr()

        assert r['compositeFilter'] == test_filter.inner_filter.to_repr()

    @staticmethod
    def test_repr_returns_to_repr_as_string(query_filter):
        assert repr(query_filter) == str(query_filter.to_repr())

    @staticmethod
    @pytest.fixture()
    def property_filters():
        # type: () -> List[PropertyFilter]
        return [
            PropertyFilter(
                prop='prop1',
                operator=PropertyFilterOperator.LESS_THAN,
                value=Value('value1')
            ),
            PropertyFilter(
                prop='prop2',
                operator=PropertyFilterOperator.GREATER_THAN,
                value=Value(1234)
            )
        ]

    @staticmethod
    @pytest.fixture()
    def composite_filter(property_filters):
        # type: (List[PropertyFilter]) -> CompositeFilter
        return CompositeFilter(
            operator=CompositeFilterOperator.AND,
            filters=[
                Filter(property_filters[0]),
                Filter(property_filters[1])
            ])

    @staticmethod
    @pytest.fixture()
    def query_filter(composite_filter):
        # type: (CompositeFilter) -> Filter
        return Filter(inner_filter=composite_filter)

    @staticmethod
    @pytest.fixture()
    def value():
        # type: () -> Value
        return Value('value')

    @staticmethod
    def _assert_is_correct_prop_dict_for_property_filter(
            prop_dict, property_filter):
        # type: (Dict[str, Any], PropertyFilter) -> None
        assert prop_dict['property']['name'] == property_filter.prop
        assert prop_dict['op'] == property_filter.operator.value
        assert prop_dict['value'] == property_filter.value.to_repr()
