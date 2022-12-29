import os

import pytest
from ckan.plugins import toolkit

from tests.helpers import patches

scenarios = [
    ('csv', {}),
    ('json', {}),
    ('xlsx', {}),
    ('dwc', {}),
    ('csv', {'delimiter': 'tab'}),
    (
        'dwc',
        {
            'core_extension_name': 'gbif_occurrence',
            'extension_names': ['gbif_multimedia'],
            'extension_map': {'gbif_multimedia': ['img']},
        },
    ),
]


@pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
@pytest.mark.usefixtures(
    'with_plugins', 'with_versioned_datastore_tables', 'with_vds_resource'
)
class TestQueueDownload:
    @patches.enqueue_job()
    @pytest.mark.parametrize('file_format,format_args', scenarios)
    @pytest.mark.parametrize('separate_files', [True, False])
    def test_run_download_without_query(
        self, enqueue_job, file_format, format_args, separate_files
    ):
        download_details = toolkit.get_action('datastore_queue_download')(
            {},
            {
                'query': {'query': {}},
                'file': {
                    'format': file_format,
                    'format_args': format_args,
                    'separate_files': separate_files,
                },
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
    def test_run_download_with_query(self, enqueue_job, with_vds_resource):
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
                        },
                    },
                    'resource_ids': [with_vds_resource['id']],
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

    @patches.enqueue_job()
    def test_run_download_ignore_empty(self, enqueue_job):
        download_details = toolkit.get_action('datastore_queue_download')(
            {},
            {
                'query': {'query': {}},
                'file': {'format': 'csv', 'ignore_empty_fields': True},
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
