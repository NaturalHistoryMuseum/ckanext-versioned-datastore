import pytest
from ckan.plugins import toolkit
from mock import patch, MagicMock


class TestQueueDownload:
    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures(
        'with_plugins', 'with_versioned_datastore_tables', 'patch_elasticsearch_scan'
    )
    def test_queue_api_call(self):
        # there is a very similar test in test_actions_downloads.py that calls this
        # directly instead
        with patch(
            'ckan.plugins.toolkit.enqueue_job', side_effect=MagicMock()
        ) as enqueue_mock, patch(
            'ckanext.versioned_datastore.lib.common.SEARCH_HELPER',
            new=MagicMock(),
        ):
            toolkit.get_action('datastore_queue_download')(
                {},
                {
                    'query': {'query': {}},
                    'file': {'format': 'csv'},
                    'notifier': {'type': 'none'},
                },
            )
            assert enqueue_mock.call_count == 1
