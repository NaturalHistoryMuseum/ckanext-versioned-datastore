import pytest
from ckan.plugins import toolkit
from collections import defaultdict, namedtuple
from datetime import datetime as dt
from mock import patch, MagicMock

Job = namedtuple('Job', ['enqueued_at', 'id'])


def sync_enqueue_download(
    job_func, args=None, kwargs=None, *queue_args, **queue_kwargs
):
    args = args or []
    kwargs = kwargs or {}
    job_func(*args, **kwargs)

    return Job(enqueued_at=dt.now(), id=1)


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
            enqueue_mock.assert_called_once()

    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures(
        'with_plugins', 'with_versioned_datastore_tables', 'patch_elasticsearch_scan'
    )
    def test_run_download(self):
        def rounded_versions_mock(indices, target_version):
            return defaultdict(lambda: target_version)

        with patch(
            'ckan.plugins.toolkit.enqueue_job', side_effect=sync_enqueue_download
        ), patch(
            'ckanext.versioned_datastore.lib.common.SEARCH_HELPER.get_rounded_versions',
            side_effect=rounded_versions_mock,
        ) as patched_rounded_versions, patch(
            'ckanext.versioned_datastore.lib.downloads.download.get_elasticsearch_client',
            side_effect=MagicMock(),
        ) as patched_elasticsearch_client:
            toolkit.get_action('datastore_queue_download')(
                {},
                {
                    'query': {'query': {}},
                    'file': {'format': 'csv'},
                    'notifier': {'type': 'none'},
                },
            )
            patched_rounded_versions.assert_called()
            patched_elasticsearch_client.assert_called()
