from unittest.mock import MagicMock, patch

import pytest
from ckan.plugins import toolkit

from ckanext.versioned_datastore.logic.download.arg_objects import QueryArgs
from tests.helpers import patches


class TestDownloadQuery:
    def test_create_query(self):
        check_mock = MagicMock(return_value=True)
        with patch(
            'ckanext.versioned_datastore.logic.validators.check_resource_id', check_mock
        ):
            with patches.query_schemas():
                q = QueryArgs(
                    query={},
                    query_version='v1.0.0',
                    resource_ids=['resource-id-here'],
                ).to_schema_query()
            assert q.query == {}
            assert q.query_version == 'v1.0.0'
            assert q.resource_ids == ['resource-id-here']

    # todo: skipped because ckantools might have a bug in it where if one validation
    #       error is raised, it tries to re-raise errors[0] but errors is a dict not a
    #       list (ckantools/validators/ivalidators.py#L87)
    @pytest.mark.skip
    def test_create_query_not_datastore(self):
        check_mock = MagicMock(return_value=False)
        with patch(
            'ckanext.versioned_datastore.logic.validators.check_datastore_resource_id',
            check_mock,
        ):
            with patches.query_schemas():
                with pytest.raises(
                    toolkit.Invalid, match='No resource IDs are datastore resources'
                ):
                    QueryArgs(
                        query={'resource_ids': ['resource-id-here']},
                        query_version='v1.0.0',
                    )

    @pytest.mark.usefixtures('with_vds')
    def test_create_query_from_query_args(self):
        resource_ids = ['test-resource-id']
        check_mock = MagicMock(return_value=True)
        with patch(
            'ckanext.versioned_datastore.logic.validators.check_datastore_resource_id',
            check_mock,
        ):
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
            q = query_args.to_schema_query()

        assert q.query_version == 'v1.0.0'
        assert q.resource_ids == resource_ids
        assert 'filters' in q.query
        assert q.hash == '83fc3087623dfd7371cf697d1de6c879de3e722a'
