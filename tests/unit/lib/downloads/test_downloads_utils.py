from operator import itemgetter

import pytest
from splitgill.model import Record

from ckanext.versioned_datastore.lib.downloads import utils
from ckanext.versioned_datastore.lib.downloads.utils import get_schema
from ckanext.versioned_datastore.lib.importing.options import (
    create_default_options_builder,
)
from ckanext.versioned_datastore.lib.utils import get_database


class TestGetSchema:
    @pytest.mark.usefixtures('with_vds')
    def test_get_schema(self):
        resource_id = 'test-resource-id'
        database = get_database(resource_id)
        database.update_options(create_default_options_builder().build(), commit=False)
        database.ingest(
            [
                Record.new(
                    {
                        'name': 'Paru',
                        'size': 5,
                        'dob': '2021-01-12',
                        'good': True,
                        'toys': ['Feather stick', 'Monsieur Canard', 'Catnip Carrot'],
                        'food': [
                            {
                                'name': 'Chicken',
                                'weight': 40,
                            },
                            {
                                'name': 'Kibble',
                                'weight': 36,
                            },
                        ],
                    }
                )
            ],
            commit=True,
        )
        database.sync()

        schema = get_schema(resource_id)

        assert isinstance(schema, dict)
        assert schema['type'] == 'record'
        assert schema['name'] == 'Record'
        assert len(schema['fields']) == 7

        assert sorted(schema['fields'], key=itemgetter('name')) == [
            {'name': '_id', 'type': ['string', 'null']},
            {'name': 'dob', 'type': ['string', 'null']},
            {
                'name': 'food',
                'type': [
                    {
                        'items': [
                            [
                                {
                                    'fields': [
                                        {'name': 'name', 'type': ['string', 'null']},
                                        {'name': 'weight', 'type': ['long', 'null']},
                                    ],
                                    'name': 'food.Record',
                                    'type': 'record',
                                },
                                'null',
                            ]
                        ],
                        'type': 'array',
                    },
                    'null',
                ],
            },
            {'name': 'good', 'type': ['boolean', 'null']},
            {'name': 'name', 'type': ['string', 'null']},
            {'name': 'size', 'type': ['long', 'null']},
            {
                'name': 'toys',
                'type': [{'items': [['string', 'null']], 'type': 'array'}, 'null'],
            },
        ]


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
