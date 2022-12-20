from mock import MagicMock, patch
import pytest

from ckanext.versioned_datastore.lib.downloads import query
from ckanext.versioned_datastore.logic.actions.meta.arg_objects import QueryArgs
from tests.helpers import patches


class TestDownloadQuery:
    def test_create_query(self):
        test_schemas = {'v1.0.0': MagicMock(validate=MagicMock(return_value=True))}
        with patch(
            'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas
        ):
            q = query.Query(
                query={},
                query_version='v1.0.0',
                resource_ids_and_versions={'resource-id-here': 0},
            )
        assert q.query == {}
        assert q.query_version == 'v1.0.0'
        assert q.resource_ids_and_versions == {'resource-id-here': 0}

    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures('with_plugins', 'with_versioned_datastore_tables')
    def test_create_query_from_query_args(self, patch_elasticsearch_scan):
        query_args = QueryArgs(
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
                },
                'resource_ids': [patch_elasticsearch_scan.return_value[0].name],
            }
        )
        test_schemas = {'v1.0.0': MagicMock(validate=MagicMock(return_value=True))}
        with patch(
            'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas
        ), patches.patch_rounded_versions():
            q = query.Query.from_query_args(query_args)

        assert q.query_version == 'v1.0.0'
