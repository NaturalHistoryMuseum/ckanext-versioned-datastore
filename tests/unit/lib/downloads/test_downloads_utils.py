import pytest
from ckan.plugins import toolkit
from ckan.tests import factories
from mock import MagicMock, patch

from ckanext.versioned_datastore.lib.downloads import utils, query
from tests.helpers import patches


class TestGetSchema:
    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures('with_plugins', 'clean_db')
    def test_get_schema(self):
        resource_dict = factories.Resource()
        with patches.query_schemas():
            q = query.Query(
                query={
                    'filters': {
                        'and': [
                            {
                                'string_equals': {
                                    'fields': ['collectionCode'],
                                    'value': 'bot',
                                }
                            }
                        ]
                    }
                },
                query_version='v1.0.0',
                resource_ids_and_versions={resource_dict['id']: 1},
            )

        index_name = (
            toolkit.config.get('ckanext.versioned_datastore.elasticsearch_index_prefix')
            + resource_dict['id']
        )

        # this is a _very_ stripped down version of the return value from
        # indices.get_mapping()
        get_mapping_mock = MagicMock(
            return_value={
                index_name: {
                    'mappings': {
                        '_doc': {
                            'properties': {
                                'data': {
                                    'properties': {
                                        '_id': {'type': 'long'},
                                        'name': {
                                            'type': 'keyword',
                                            'fields': {
                                                'full': {'type': 'text'},
                                                'number': {
                                                    'type': 'double',
                                                    'ignore_malformed': True,
                                                },
                                            },
                                            'copy_to': ['meta.all'],
                                            'ignore_above': 256,
                                            'normalizer': 'lowercase_normalizer',
                                        },
                                        'modified': {
                                            'type': 'date',
                                            'format': 'epoch_millis',
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )
        with patch(
            "ckanext.versioned_datastore.lib.downloads.utils.get_mappings",
            get_mapping_mock,
        ):
            parsed_schemas = utils.get_schemas(q)
        parsed_schema = parsed_schemas[resource_dict["id"]]
        assert isinstance(parsed_schema, dict)
        assert parsed_schema['type'] == 'record'
        assert parsed_schema['name'] == 'ResourceRecord'
        assert len(parsed_schema['fields']) == 3
        assert isinstance(parsed_schema['fields'][0]['type'], list)


class TestFilterDataFields:
    def test_excludes_field_with_zero_count(self):
        data = {'one': 'a', 'two': 'b', 'three': 'c'}
        field_counts = {'one': 1, 'two': 1, 'three': 0}

        filtered_data = utils.filter_data_fields(data, field_counts)
        assert 'one' in filtered_data
        assert 'two' in filtered_data
        assert 'three' not in filtered_data

    def test_excludes_field_with_no_count(self):
        data = {'one': 'a', 'two': 'b', 'three': 'c'}
        field_counts = {'one': 1, 'two': 1}

        filtered_data = utils.filter_data_fields(data, field_counts)
        assert 'one' in filtered_data
        assert 'two' in filtered_data
        assert 'three' not in filtered_data

    def test_includes_null_field_with_count(self):
        data = {'one': 'a', 'two': 'b', 'three': None}
        field_counts = {'one': 1, 'two': 1, 'three': 1}

        filtered_data = utils.filter_data_fields(data, field_counts)
        assert 'one' in filtered_data
        assert 'two' in filtered_data
        assert 'three' in filtered_data

    def test_excludes_empty_nested_fields(self):
        data = {'one': {'two': 'a', 'three': 'b'}}
        field_counts = {'one.two': 1}

        filtered_data = utils.filter_data_fields(data, field_counts)
        assert 'one' in filtered_data
        assert 'two' in filtered_data['one']
        assert 'three' not in filtered_data['one']

    def test_excludes_empty_listed_fields(self):
        data = {'one': [{'two': 'a', 'three': 'b'}, {'two': 'c', 'four': 'd'}]}
        field_counts = {'one.two': 2, 'one.four': 1}

        filtered_data = utils.filter_data_fields(data, field_counts)
        assert 'one' in filtered_data
        assert len(filtered_data['one']) == 2
        assert 'four' in filtered_data['one'][1]
        for r in filtered_data['one']:
            assert 'two' in r
            assert 'three' not in r


class TestFlattenDict:
    def test_flattens_dict(self):
        data = {'one': {'two': 2, 'three': 3}}
        expected_output = {'one.two': 2, 'one.three': 3}

        flattened = utils.flatten_dict(data)
        assert len(flattened) == len(expected_output)
        for k, v in expected_output.items():
            assert k in flattened
            assert v == flattened[k]

    def test_flattens_nested_lists(self):
        data = {'one': [{'two': 'a', 'three': 'b'}, {'two': 'c', 'four': 'd'}]}
        expected_output = {'one.two': 'a | c', 'one.three': 'b', 'one.four': 'd'}

        flattened = utils.flatten_dict(data)
        assert len(flattened) == len(expected_output)
        for k, v in expected_output.items():
            assert k in flattened
            assert v == flattened[k]

    def test_flattens_lists(self):
        data = {'one': ['a', 'b']}
        expected_output = {'one': 'a | b'}

        flattened = utils.flatten_dict(data)
        assert len(flattened) == len(expected_output)
        for k, v in expected_output.items():
            assert k in flattened
            assert v == flattened[k]
