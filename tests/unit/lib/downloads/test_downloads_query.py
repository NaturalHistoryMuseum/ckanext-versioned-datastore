import pytest

from ckanext.versioned_datastore.lib.downloads import query
from ckanext.versioned_datastore.logic.actions.meta.arg_objects import QueryArgs
from tests.helpers import patches


class TestDownloadQuery:
    def test_create_query(self):
        with patches.query_schemas():
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
    def test_create_query_from_query_args(self):
        resource_ids = ['test-resource-id']
        with patches.get_available_resources(
            resource_ids
        ), patches.query_schemas(), patches.rounded_versions():
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
                    'resource_ids': resource_ids,
                }
            )
            q = query.Query.from_query_args(query_args)

        assert q.query_version == 'v1.0.0'
        assert len(q.resource_ids_and_versions) == len(resource_ids)
        assert sorted(q.resource_ids_and_versions.keys()) == resource_ids
        assert 'filters' in q.query
        assert q.hash == '83fc3087623dfd7371cf697d1de6c879de3e722a'
