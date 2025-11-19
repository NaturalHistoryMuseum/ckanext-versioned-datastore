from datetime import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from ckan.plugins import toolkit

from ckanext.versioned_datastore.logic.slug.action import vds_slug_resolve

warnings = {
    'NO_RESOURCES': 'Resource IDs were not saved for this search. The list of '
    'available resources may have changed since this slug was saved.',
    'ALL_INVALID_RESOURCES': 'All resources associated with this search have been '
    'deleted, moved, or are no longer available.',
    'SOME_INVALID_RESOURCES': 'Some resources have been deleted, moved, or are no '
    'longer available. Affected resources: {}',
    'RECORD_COUNT': 'Record count at save time: {}',
}


@pytest.mark.usefixtures('with_request_context', 'with_vds', 'with_vds_resource')
class TestResolveSlug:
    @patch('ckanext.versioned_datastore.logic.slug.action.resolve_slug')
    def test_resolve_slug_basic(self, mock_resolve_slug, with_vds_resource):
        """
        Test resolving a regular vds slug with no modifications or warnings.
        """
        resource_1, resource_2 = with_vds_resource
        ts = dt(2025, 1, 1)
        query = {'search': 'banana'}
        mock_resolve_slug.return_value = MagicMock(
            query=query,
            query_version='v1.0.0',
            resource_ids=[
                resource_1['id'],
                resource_2['id'],
            ],
            version=1234,
            created=ts,
        )
        action_response = vds_slug_resolve('test-slug')

        assert action_response['query'] == query
        assert action_response['query_version'] == 'v1.0.0'
        assert resource_1['id'] in action_response['resource_ids']
        assert resource_2['id'] in action_response['resource_ids']
        assert len(action_response['resource_ids']) == 2
        assert action_response['version'] == 1234
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 0

    @patch('ckanext.versioned_datastore.logic.slug.action.resolve_slug')
    def test_resolve_slug_v0(self, mock_resolve_slug, with_vds_resource):
        """
        Test resolving a v0 vds slug.
        """
        resource_1, resource_2 = with_vds_resource
        ts = dt(2025, 1, 1)
        v0_query = {'filters': {'_id': ['1']}}
        v1_query = {
            'filters': {'and': [{'string_equals': {'fields': ['_id'], 'value': '1'}}]}
        }
        mock_resolve_slug.return_value = MagicMock(
            query=v0_query,
            query_version='v0',
            resource_ids=[
                resource_1['id'],
                resource_2['id'],
            ],
            version=1234,
            created=ts,
        )
        action_response = vds_slug_resolve('test-slug')

        assert action_response['query'] == v1_query
        assert action_response['query_version'] == 'v1.0.0'  # current version
        assert resource_1['id'] in action_response['resource_ids']
        assert resource_2['id'] in action_response['resource_ids']
        assert len(action_response['resource_ids']) == 2
        assert action_response['version'] == 1234
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 0

    @patch('ckanext.versioned_datastore.logic.slug.action.resolve_slug')
    def test_resolve_slug_no_resources(self, mock_resolve_slug, with_vds_resource):
        """
        Test resolving a vds slug with no resources.
        """
        ts = dt(2025, 1, 1)
        mock_resolve_slug.return_value = MagicMock(
            query={},
            query_version='v1.0.0',
            resource_ids=[],
            version=1234,
            created=ts,
        )
        action_response = vds_slug_resolve('test-slug')

        assert action_response['query'] == {}
        assert action_response['query_version'] == 'v1.0.0'
        assert len(action_response['resource_ids']) == 0
        assert action_response['version'] == 1234
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 1
        assert warnings['NO_RESOURCES'] in action_response['warnings']

    @patch('ckanext.versioned_datastore.logic.slug.action.resolve_slug')
    def test_resolve_slug_some_invalid_resources(
        self, mock_resolve_slug, with_vds_resource
    ):
        """
        Test resolving a vds slug where some but not all of the resources are invalid.
        """
        resource_1, resource_2 = with_vds_resource
        ts = dt(2025, 1, 1)
        mock_resolve_slug.return_value = MagicMock(
            query={},
            query_version='v1.0.0',
            resource_ids=[
                resource_1['id'],
                'invalid-resource-id',
            ],
            version=1234,
            created=ts,
        )
        action_response = vds_slug_resolve('test-slug')

        assert action_response['query'] == {}
        assert action_response['query_version'] == 'v1.0.0'
        assert resource_1['id'] in action_response['resource_ids']
        assert 'invalid-resource-id' not in action_response['resource_ids']
        assert len(action_response['resource_ids']) == 1
        assert action_response['version'] == 1234
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 1
        assert (
            warnings['SOME_INVALID_RESOURCES'].format(str(1))
            in action_response['warnings']
        )

    @patch('ckanext.versioned_datastore.logic.slug.action.resolve_slug')
    def test_resolve_slug_all_invalid_resources(self, mock_resolve_slug):
        """
        Test resolving a vds slug where all of the resources are invalid.
        """
        ts = dt(2025, 1, 1)
        mock_resolve_slug.return_value = MagicMock(
            query={},
            query_version='v1.0.0',
            resource_ids=[
                'invalid-resource-id',
            ],
            version=1234,
            created=ts,
        )
        with pytest.raises(
            toolkit.ValidationError, match=warnings['ALL_INVALID_RESOURCES']
        ):
            vds_slug_resolve('test-slug')

    @patch(
        'ckanext.versioned_datastore.logic.slug.action.plugin_loaded',
        return_value=False,
    )
    @patch(
        'ckanext.versioned_datastore.logic.slug.action.resolve_slug', return_value=None
    )
    def test_no_slug(self, *args):
        """
        Test trying to resolve a string that is neither a slug nor a DOI.
        """
        with pytest.raises(
            toolkit.ValidationError, match='This saved search could not be found'
        ):
            vds_slug_resolve('slug-does-not-exist')


@pytest.mark.usefixtures('with_request_context', 'with_vds', 'with_vds_resource')
class TestResolveDOI:
    @patch(
        'ckanext.versioned_datastore.logic.slug.action.plugin_loaded', return_value=True
    )
    @patch(
        'ckanext.versioned_datastore.logic.slug.action.resolve_slug', return_value=None
    )
    @patch('ckanext.query_dois.model.QueryDOI')  # we're not testing this
    def test_resolve_doi_basic(
        self, mock_plugin_loaded, mock_resolve_slug, mock_query_doi, with_vds_resource
    ):
        """
        Test resolving a query DOI.
        """
        resource_1, resource_2 = with_vds_resource
        ts = dt(2025, 1, 1)
        query = {'search': 'banana'}
        mock_resolved = MagicMock(
            query=query,
            query_version='v1.0.0',
            requested_version=1357,
            timestamp=ts,
            count=1000,
        )
        mock_resolved.get_rounded_versions.return_value = [1234, 5678]
        mock_resolved.get_resource_ids.return_value = [
            resource_1['id'],
            resource_2['id'],
        ]
        with patch('ckan.model.Session') as mock_session:
            mock_session.query.return_value.filter.return_value.first.return_value = (
                mock_resolved
            )
            action_response = vds_slug_resolve('test-doi')

        assert action_response['query'] == query
        assert action_response['query_version'] == 'v1.0.0'
        assert resource_1['id'] in action_response['resource_ids']
        assert resource_2['id'] in action_response['resource_ids']
        assert len(action_response['resource_ids']) == 2
        assert action_response['version'] == 1357
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 0

    @patch(
        'ckanext.versioned_datastore.logic.slug.action.plugin_loaded', return_value=True
    )
    @patch(
        'ckanext.versioned_datastore.logic.slug.action.resolve_slug', return_value=None
    )
    @patch('ckanext.query_dois.model.QueryDOI')  # we're not testing this
    def test_resolve_doi_no_requested_version(
        self, mock_plugin_loaded, mock_resolve_slug, mock_query_doi, with_vds_resource
    ):
        """
        Test resolving a query DOI where the requested_version has not been specified.
        """
        resource_1, resource_2 = with_vds_resource
        ts = dt(2025, 1, 1)
        query = {'search': 'banana'}
        mock_resolved = MagicMock(
            query=query,
            query_version='v1.0.0',
            requested_version=None,
            timestamp=ts,
            count=1000,
        )
        mock_resolved.get_rounded_versions.return_value = [1234, 5678]
        mock_resolved.get_resource_ids.return_value = [
            resource_1['id'],
            resource_2['id'],
        ]
        with patch('ckan.model.Session') as mock_session:
            mock_session.query.return_value.filter.return_value.first.return_value = (
                mock_resolved
            )
            action_response = vds_slug_resolve('test-doi')

        assert action_response['query'] == query
        assert action_response['query_version'] == 'v1.0.0'
        assert resource_1['id'] in action_response['resource_ids']
        assert resource_2['id'] in action_response['resource_ids']
        assert len(action_response['resource_ids']) == 2
        assert action_response['version'] == 5678
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 0

    @patch(
        'ckanext.versioned_datastore.logic.slug.action.plugin_loaded', return_value=True
    )
    @patch(
        'ckanext.versioned_datastore.logic.slug.action.resolve_slug', return_value=None
    )
    @patch('ckanext.query_dois.model.QueryDOI')  # we're not testing this
    def test_resolve_doi_v0(
        self, mock_plugin_loaded, mock_resolve_slug, mock_query_doi, with_vds_resource
    ):
        """
        Test resolving a v0 query DOI.
        """
        resource_1, resource_2 = with_vds_resource
        ts = dt(2025, 1, 1)
        v0_query = {'filters': {'_id': ['1']}}
        v1_query = {
            'filters': {'and': [{'string_equals': {'fields': ['_id'], 'value': '1'}}]}
        }
        mock_resolved = MagicMock(
            query=v0_query,
            query_version='v0',
            requested_version=1357,
            timestamp=ts,
            count=1000,
        )
        mock_resolved.get_rounded_versions.return_value = [1234, 5678]
        mock_resolved.get_resource_ids.return_value = [
            resource_1['id'],
            resource_2['id'],
        ]
        with patch('ckan.model.Session') as mock_session:
            mock_session.query.return_value.filter.return_value.first.return_value = (
                mock_resolved
            )
            action_response = vds_slug_resolve('test-doi')

        assert action_response['query'] == v1_query
        assert action_response['query_version'] == 'v1.0.0'
        assert resource_1['id'] in action_response['resource_ids']
        assert resource_2['id'] in action_response['resource_ids']
        assert len(action_response['resource_ids']) == 2
        assert action_response['version'] == 1357
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 0

    @patch(
        'ckanext.versioned_datastore.logic.slug.action.plugin_loaded', return_value=True
    )
    @patch(
        'ckanext.versioned_datastore.logic.slug.action.resolve_slug', return_value=None
    )
    @patch('ckanext.query_dois.model.QueryDOI')  # we're not testing this
    def test_resolve_doi_some_invalid_resources(
        self, mock_plugin_loaded, mock_resolve_slug, mock_query_doi, with_vds_resource
    ):
        """
        Test resolving a query DOI with some invalid resources.
        """
        resource_1, resource_2 = with_vds_resource
        ts = dt(2025, 1, 1)
        mock_resolved = MagicMock(
            query={},
            query_version='v1.0.0',
            requested_version=1357,
            timestamp=ts,
            count=1000,
        )
        mock_resolved.get_rounded_versions.return_value = [1234, 5678]
        mock_resolved.get_resource_ids.return_value = [
            resource_1['id'],
            'invalid-resource-id',
        ]
        with patch('ckan.model.Session') as mock_session:
            mock_session.query.return_value.filter.return_value.first.return_value = (
                mock_resolved
            )
            action_response = vds_slug_resolve('test-doi')

        assert action_response['query'] == {}
        assert action_response['query_version'] == 'v1.0.0'
        assert resource_1['id'] in action_response['resource_ids']
        assert 'invalid-resource-id' not in action_response['resource_ids']
        assert len(action_response['resource_ids']) == 1
        assert action_response['version'] == 1357
        assert action_response['created'] == '2025-01-01T00:00:00'
        assert len(action_response['warnings']) == 2
        assert (
            warnings['SOME_INVALID_RESOURCES'].format(str(1))
            in action_response['warnings']
        )
        assert warnings['RECORD_COUNT'].format(str(1000)) in action_response['warnings']

    @patch(
        'ckanext.versioned_datastore.logic.slug.action.plugin_loaded', return_value=True
    )
    @patch(
        'ckanext.versioned_datastore.logic.slug.action.resolve_slug', return_value=None
    )
    @patch('ckanext.query_dois.model.QueryDOI')  # we're not testing this
    def test_resolve_doi_all_invalid_resources(
        self, mock_plugin_loaded, mock_resolve_slug, mock_query_doi, with_vds_resource
    ):
        """
        Test resolving a query DOI with no valid resources.
        """
        ts = dt(2025, 1, 1)
        mock_resolved = MagicMock(
            query={},
            query_version='v1.0.0',
            requested_version=1357,
            timestamp=ts,
            count=1000,
        )
        mock_resolved.get_rounded_versions.return_value = [1234, 5678]
        mock_resolved.get_resource_ids.return_value = [
            'invalid-resource-id',
        ]
        with patch('ckan.model.Session') as mock_session:
            mock_session.query.return_value.filter.return_value.first.return_value = (
                mock_resolved
            )
            with pytest.raises(
                toolkit.ValidationError, match=warnings['ALL_INVALID_RESOURCES']
            ):
                vds_slug_resolve('test-doi')
