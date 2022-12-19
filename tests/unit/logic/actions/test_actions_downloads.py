import pytest
from mock import patch, MagicMock

from ckanext.versioned_datastore.logic.actions.downloads import datastore_queue_download
from ckanext.versioned_datastore.logic.actions.meta.arg_objects import (
    QueryArgs,
    DerivativeArgs,
    NotifierArgs,
)


class TestQueueDownload:
    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures(
        'with_plugins', 'with_versioned_datastore_tables', 'patch_elasticsearch_scan'
    )
    def test_queue_direct_call(self):
        # there is a very similar test in test_downloads.py that calls this via the API
        # instead
        with patch(
            'ckan.plugins.toolkit.enqueue_job', side_effect=MagicMock()
        ) as enqueue_mock, patch(
            'ckanext.versioned_datastore.lib.common.SEARCH_HELPER',
            new=MagicMock(),
        ):
            datastore_queue_download(
                {},
                QueryArgs(),
                DerivativeArgs(format='csv'),
                notifier=NotifierArgs(type='none'),
            )
            assert enqueue_mock.call_count == 1
