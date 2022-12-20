import pytest
from ckan.plugins import toolkit
from ckan.tests import factories
from mock import MagicMock, patch

from ckanext.versioned_datastore.lib.downloads import utils, query


class TestGetSchema:
    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures('with_plugins', 'clean_db')
    def test_get_schema(self):
        resource_dict = factories.Resource()
        test_schemas = {'v1.0.0': MagicMock(validate=MagicMock(return_value=True))}
        with patch(
            'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas
        ):
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
        indices_mock = MagicMock(
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
        parsed_schema = utils.get_schema(
            q, MagicMock(**{'indices.get_mapping': indices_mock})
        )
        assert isinstance(parsed_schema, dict)
        assert parsed_schema['type'] == 'record'
        assert parsed_schema['name'] == 'ResourceRecord'
        assert len(parsed_schema['fields']) == 3
        assert isinstance(parsed_schema['fields'][0]['type'], list)
