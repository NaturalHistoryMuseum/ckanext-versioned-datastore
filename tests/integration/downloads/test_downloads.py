import os
from collections import namedtuple
from datetime import datetime as dt

import pytest
from ckan.plugins import toolkit
from mock import patch, MagicMock

from tests.helpers import patches

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
    def test_run_basic_download(self):
        with patch(
            'ckan.plugins.toolkit.enqueue_job', side_effect=sync_enqueue_download
        ), patches.patch_rounded_versions() as patched_rounded_versions, patch(
            'ckanext.versioned_datastore.lib.downloads.download.get_elasticsearch_client',
            side_effect=MagicMock(),
        ) as patched_elasticsearch_client:
            download_details = toolkit.get_action('datastore_queue_download')(
                {},
                {
                    'query': {'query': {}},
                    'file': {'format': 'csv'},
                    'notifier': {'type': 'none'},
                },
            )
            patched_rounded_versions.assert_called()
            patched_elasticsearch_client.assert_called()
            download_dir = toolkit.config.get(
                'ckanext.versioned_datastore.download_dir'
            )
            assert any(
                [
                    f.startswith(download_details['download_id'])
                    for f in os.listdir(download_dir)
                ]
            )

    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures(
        'with_plugins', 'with_versioned_datastore_tables', 'patch_elasticsearch_scan'
    )
    def test_run_download_with_query(self):
        with patch(
            'ckan.plugins.toolkit.enqueue_job', side_effect=sync_enqueue_download
        ), patches.patch_rounded_versions() as patched_rounded_versions, patch(
            'ckanext.versioned_datastore.lib.downloads.download.get_elasticsearch_client',
            side_effect=MagicMock(),
        ) as patched_elasticsearch_client:
            download_details = toolkit.get_action('datastore_queue_download')(
                {},
                {
                    'query': {
                        'query': {
                            'filters': {
                                'and': [
                                    {
                                        'string_equals': {
                                            'fields': ['colour'],
                                            'value': 'green',
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    'file': {'format': 'csv'},
                    'notifier': {'type': 'none'},
                },
            )
            patched_rounded_versions.assert_called()
            patched_elasticsearch_client.assert_called()
            download_dir = toolkit.config.get(
                'ckanext.versioned_datastore.download_dir'
            )
            assert any(
                [
                    f.startswith(download_details['download_id'])
                    for f in os.listdir(download_dir)
                ]
            )
