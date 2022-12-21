import os

import pytest
from ckan.plugins import toolkit

from tests.helpers import patches


@pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
@pytest.mark.usefixtures(
    'with_plugins', 'with_versioned_datastore_tables', 'with_vds_resource'
)
class TestQueueDownload:
    @patches.enqueue_job()
    @pytest.mark.parametrize('file_format', ['csv', 'json', 'xlsx', 'dwc'])
    def test_run_basic_download(self, enqueue_job, file_format):
        download_details = toolkit.get_action('datastore_queue_download')(
            {},
            {
                'query': {'query': {}},
                'file': {'format': file_format},
                'notifier': {'type': 'none'},
            },
        )
        enqueue_job.assert_called()
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        assert any(
            [
                f.startswith(download_details['download_id'])
                for f in os.listdir(download_dir)
            ]
        )

    @patches.enqueue_job()
    def test_run_download_with_query(self, enqueue_job):
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
        enqueue_job.assert_called()
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        assert any(
            [
                f.startswith(download_details['download_id'])
                for f in os.listdir(download_dir)
            ]
        )
