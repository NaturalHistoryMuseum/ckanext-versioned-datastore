import os

import pytest
from ckan.plugins import toolkit
from mock import patch, MagicMock
import zipfile

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

expected_extensions = {'csv': '.csv', 'json': '.json', 'xlsx': '.xlsx', 'dwc': '.zip'}


@pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
@pytest.mark.usefixtures(
    'with_plugins',
    'with_versioned_datastore_tables',
    'with_vds_resource',
    'clear_download_dir',
)
class TestQueueDownload:
    @patches.enqueue_job()
    @pytest.mark.parametrize('file_format,format_args', scenarios)
    @pytest.mark.parametrize('separate_files', [True, False])
    def test_run_download_without_query(
        self, enqueue_job, with_vds_resource, file_format, format_args, separate_files
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
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_details['download_id'])
        ]
        assert len(matching_zips) == 1
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            if not separate_files:
                assert f'resource{expected_extensions[file_format]}' in archive_files
                assert len(archive_files) == 2
            else:
                assert (
                    f'{with_vds_resource["id"]}{expected_extensions[file_format]}'
                    in archive_files
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

    @patches.enqueue_job()
    @pytest.mark.parametrize('transform', [{'id_as_url': {'field': 'urlSlug'}}])
    def test_run_download_with_transform(self, enqueue_job, transform):
        with patch('ckan.plugins.toolkit.url_for', return_value='/banana'):
            download_details = toolkit.get_action('datastore_queue_download')(
                {},
                {
                    'query': {'query': {}},
                    'file': {'format': 'csv', 'transform': transform},
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


@pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore query_dois')
@pytest.mark.ckan_config('ckanext.query_dois.prefix', 'xx.xxxx')
@pytest.mark.usefixtures(
    'with_plugins',
    'with_versioned_datastore_tables',
    'with_vds_resource',
    'clear_download_dir',
)
class TestDownloadWithQueryDois:
    @classmethod
    def setup_class(cls):
        from ckanext.query_dois.model import query_doi_table, query_doi_stat_table

        cls.tables = [query_doi_table, query_doi_stat_table]

        for table in cls.tables:
            table.create(checkfirst=True)

    @classmethod
    def teardown_class(cls):
        for table in cls.tables:
            if table.exists():
                table.drop()

    @patches.enqueue_job()
    def test_run_download_with_query_dois(self, enqueue_job):

        # I cannot get the query_doi endpoints to load for the life of me so we're just
        # going to mock it before I lose my mind
        def _url_for_mock(endpoint, **kwargs):
            if endpoint == 'query_doi.landing_page':
                return f'/{kwargs["data_centre"]}/{kwargs["identifier"]}'
            else:
                return toolkit.url_for(endpoint, **kwargs)

        with patch(
            'ckanext.query_dois.lib.doi.find_existing_doi',
            return_value=MagicMock(doi='123456'),
        ), patch('ckan.plugins.toolkit.url_for', _url_for_mock):
            download_details = toolkit.get_action('datastore_queue_download')(
                {},
                {
                    'query': {'query': {}},
                    'file': {'format': 'dwc'},
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
