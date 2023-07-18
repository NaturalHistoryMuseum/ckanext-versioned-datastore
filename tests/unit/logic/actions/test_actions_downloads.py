import pytest
from mock import patch

from ckanext.versioned_datastore.logic.actions.downloads import datastore_queue_download
from ckanext.versioned_datastore.logic.actions.meta.arg_objects import (
    QueryArgs,
    DerivativeArgs,
    NotifierArgs,
)
from tests.helpers import patches


class TestQueueDownload:
    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures('with_plugins', 'with_versioned_datastore_tables')
    def test_queue_direct_call(self):
        # there is a very similar test in test_downloads.py that calls this via the API
        # instead
        resource_ids = ['test-resource-id']

        with patch(
            'ckan.plugins.toolkit.enqueue_job'
        ) as enqueue_mock, patches.rounded_versions(), patches.get_available_resources(
            resource_ids
        ), patches.url_for():
            datastore_queue_download(
                {},
                QueryArgs(),
                DerivativeArgs(format='csv'),
                notifier=NotifierArgs(type='none'),
            )
            assert enqueue_mock.call_count == 1
