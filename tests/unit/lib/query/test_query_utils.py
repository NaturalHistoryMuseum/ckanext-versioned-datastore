from ckanext.versioned_datastore.lib.query.utils import (
    convert_small_or_groups,
    remove_empty_groups,
)


class TestQueryUtils(object):
    def test_converts_single_item_or_group(self):
        test_query = {
            'filters': {'or': [{'string_equals': {'fields': ['x'], 'value': 'y'}}]}
        }
        expected_query = {
            'filters': {'and': [{'string_equals': {'fields': ['x'], 'value': 'y'}}]}
        }
        output_query = convert_small_or_groups(test_query)
        assert output_query == expected_query

    def test_converts_nested_single_item_or_group(self):
        test_query = {'filters': {'and': [{'or': [{'exists': {'fields': ['x']}}]}]}}
        expected_query = {
            'filters': {'and': [{'and': [{'exists': {'fields': ['x']}}]}]}
        }
        output_query = convert_small_or_groups(test_query)
        assert output_query == expected_query

    def test_does_not_convert_multi_item_or_group(self):
        test_query = {
            'filters': {
                'or': [{'exists': {'fields': ['x']}}, {'exists': {'fields': ['y']}}]
            }
        }
        output_query = convert_small_or_groups(test_query)
        assert output_query == test_query

    def test_does_not_convert_empty_or_group(self):
        test_query = {'filters': {'or': []}}
        output_query = convert_small_or_groups(test_query)
        assert output_query == test_query

    def test_removes_empty_groups(self):
        test_query = {
            'filters': {'and': [{'or': []}, {'not': [{'exists': {'fields': ['x']}}]}]}
        }
        expected_query = {
            'filters': {'and': [{'not': [{'exists': {'fields': ['x']}}]}]}
        }
        output_query = remove_empty_groups(test_query)
        assert output_query == expected_query

    def test_removes_filters_if_root_empty(self):
        test_query = {'filters': {'and': []}}
        expected_query = {}
        output_query = remove_empty_groups(test_query)
        assert output_query == expected_query
