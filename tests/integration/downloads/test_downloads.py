import csv
import json
import os
import shutil
import tempfile
import zipfile

import pytest
from ckan.plugins import toolkit
from ckan.tests import factories
from mock import MagicMock, patch

from ckanext.versioned_datastore.model.downloads import DownloadRequest
from tests.helpers import data as test_data
from tests.helpers import patches

scenarios = [
    ('csv', {}),
    ('json', {}),
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

expected_extensions = {'csv': '.csv', 'json': '.json', 'dwc': '.zip'}


@pytest.mark.usefixtures('with_vds')
class TestQueueDownload:
    @patches.enqueue_job()
    @pytest.mark.parametrize('file_format,format_args', scenarios)
    @pytest.mark.parametrize('separate_files', [True, False])
    def test_run_download_formats(
        self, enqueue_job, with_vds_resource, file_format, format_args, separate_files
    ):
        resource_1_id = with_vds_resource[0]['id']
        resource_2_id = with_vds_resource[1]['id']

        with patches.url_for():
            download_details = toolkit.get_action('vds_download_queue')(
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
        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            if not separate_files:
                assert f'resource{expected_extensions[file_format]}' in archive_files
                assert len(archive_files) == 2
            else:
                assert len(archive_files) == 3
                assert (
                    f'{resource_1_id}{expected_extensions[file_format]}'
                    in archive_files
                )
                assert (
                    f'{resource_2_id}{expected_extensions[file_format]}'
                    in archive_files
                )

    @patches.enqueue_job()
    def test_run_download_without_query(self, enqueue_job, with_vds_resource):
        with patches.url_for():
            download_details = toolkit.get_action('vds_download_queue')(
                {},
                {
                    'query': {'query': {}},
                    'file': {'format': 'csv'},
                    'notifier': {'type': 'none'},
                },
            )
        enqueue_job.assert_called()
        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        self.temp_dir = tempfile.mktemp()
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            print(archive_files)
            assert 'manifest.json' in archive_files
            zf.extract('manifest.json', self.temp_dir)
            assert 'resource.csv' in archive_files
            zf.extract('resource.csv', self.temp_dir)

        with open(os.path.join(self.temp_dir, 'manifest.json')) as f:
            manifest = json.load(f)
            assert manifest['download_id'] == download_details['download_id']
            assert manifest['file_format'] == 'csv'
            assert sorted(manifest['files']) == sorted(archive_files)
            assert not manifest['ignore_empty_fields']
            assert not manifest['separate_files']
            assert manifest['total_records'] == len(
                test_data.records + test_data.records_addtl
            )

        with open(os.path.join(self.temp_dir, 'resource.csv')) as f:
            reader = csv.DictReader(f)
            records = [row for row in reader]
            assert len(records) == len(test_data.records + test_data.records_addtl)
            for record in records:
                assert 'emptyField' in record
                test_data_record = next(
                    r
                    for r in test_data.records + test_data.records_addtl
                    if r['scientificName'] == record['scientificName']
                )
                for k, v in test_data_record.items():
                    assert v == record[k]

        shutil.rmtree(self.temp_dir)

    @patches.enqueue_job()
    def test_run_download_with_query(self, enqueue_job, with_vds_resource):
        with patches.url_for():
            download_details = toolkit.get_action('vds_download_queue')(
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
                        }
                    },
                    'file': {'format': 'csv'},
                    'notifier': {'type': 'none'},
                },
            )
        enqueue_job.assert_called()
        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        self.temp_dir = tempfile.mktemp()
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            assert 'resource.csv' in archive_files
            zf.extract('resource.csv', self.temp_dir)

        with open(os.path.join(self.temp_dir, 'resource.csv')) as f:
            reader = csv.DictReader(f)
            records = [row for row in reader]
            assert len(records) == 1  # the number of records that should match the q

        shutil.rmtree(self.temp_dir)

    @patches.enqueue_job()
    def test_run_download_keep_empty(self, enqueue_job, with_vds_resource):
        with patches.url_for():
            download_details = toolkit.get_action('vds_download_queue')(
                {},
                {
                    'query': {
                        'query': {
                            'filters': {
                                'and': [
                                    {
                                        'string_equals': {
                                            'fields': ['group'],
                                            'value': 'b',
                                        }
                                    }
                                ]
                            },
                        }
                    },
                    'file': {
                        'format': 'csv',
                        'ignore_empty_fields': False,
                        'separate_files': False,
                    },
                    'notifier': {'type': 'none'},
                },
            )
        enqueue_job.assert_called()
        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        self.temp_dir = tempfile.mktemp()
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            assert 'resource.csv' in archive_files
            zf.extract('resource.csv', self.temp_dir)

        with open(os.path.join(self.temp_dir, 'resource.csv')) as f:
            reader = csv.reader(f)
            header = [row for row in reader][0]
            assert 'emptyField' in header

        shutil.rmtree(self.temp_dir)

    @patches.enqueue_job()
    def test_run_download_ignore_empty(self, enqueue_job, with_vds_resource):
        with patches.url_for():
            download_details = toolkit.get_action('vds_download_queue')(
                {},
                {
                    'query': {
                        'query': {
                            'filters': {
                                'and': [
                                    {
                                        'string_equals': {
                                            'fields': ['group'],
                                            'value': 'b',
                                        }
                                    }
                                ]
                            },
                        }
                    },
                    'file': {
                        'format': 'csv',
                        'ignore_empty_fields': True,
                        'separate_files': False,
                    },
                    'notifier': {'type': 'none'},
                },
            )
        enqueue_job.assert_called()
        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        self.temp_dir = tempfile.mktemp()
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            assert 'resource.csv' in archive_files
            zf.extract('resource.csv', self.temp_dir)

        with open(os.path.join(self.temp_dir, 'resource.csv')) as f:
            reader = csv.reader(f)
            header = [row for row in reader][0]
            assert 'emptyField' not in header

        shutil.rmtree(self.temp_dir)

    @patches.enqueue_job()
    @pytest.mark.parametrize('transform', [{'id_as_url': {'field': 'urlSlug'}}])
    def test_run_download_with_transform(
        self, enqueue_job, transform, with_vds_resource
    ):
        with patches.url_for():
            download_details = toolkit.get_action('vds_download_queue')(
                {},
                {
                    'query': {'query': {}},
                    'file': {'format': 'csv', 'transform': transform},
                    'notifier': {'type': 'none'},
                },
            )
        enqueue_job.assert_called()
        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        self.temp_dir = tempfile.mktemp()
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            assert 'resource.csv' in archive_files
            zf.extract('resource.csv', self.temp_dir)

        with open(os.path.join(self.temp_dir, 'resource.csv')) as f:
            reader = csv.DictReader(f)
            records = [row for row in reader]
            assert len(records) == len(test_data.records + test_data.records_addtl)
            for record in records:
                assert record['urlSlug'].endswith('/banana')

        shutil.rmtree(self.temp_dir)

    @patches.enqueue_job()
    def test_run_download_with_non_vds_resource(self, enqueue_job, with_vds_resource):
        with patch(
            'ckanext.versioned_datastore.logic.data.action.is_resource_read_only',
            MagicMock(return_value=True),
        ):
            non_ds_resource = factories.Resource(url_type='upload')

        def _shutil_mock(src, dest):
            # there's nothing to copy from, so just write the new file
            with open(dest, 'w') as f:
                f.write('hello')

        with patch('shutil.copy2', side_effect=_shutil_mock), patches.url_for():
            download_details = toolkit.get_action('vds_download_queue')(
                {},
                {
                    'query': {
                        'query': {},
                        'resource_ids': [non_ds_resource['id']],
                    },
                    'file': {
                        'format': 'raw',
                        'format_args': {'allow_non_datastore': True},
                    },
                    'notifier': {'type': 'none'},
                },
            )
        enqueue_job.assert_called()
        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        self.temp_dir = tempfile.mktemp()
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            zf.extract('manifest.json', self.temp_dir)
            assert len(archive_files) == 2
            # it's easier to do it this way than to predict the url or split it or whatever
            extless_files = [os.path.splitext(f)[0] for f in archive_files]
            assert non_ds_resource['id'] in extless_files

        with open(os.path.join(self.temp_dir, 'manifest.json')) as f:
            manifest = json.load(f)
            assert manifest['download_id'] == download_details['download_id']
            assert manifest['file_format'] == 'raw'
            assert sorted(manifest['files']) == sorted(archive_files)
            assert not manifest['ignore_empty_fields']
            assert manifest['separate_files']
            assert manifest['total_records'] == 1

        shutil.rmtree(self.temp_dir)


@pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
@pytest.mark.usefixtures(
    'with_plugins',
    'with_vds',
    'with_vds_resource',
)
@patches.enqueue_job()
class TestDownloadInterfaces:
    def test_modify_args(self, enqueue_job):
        mock_plugin = ModifyArgsPlugin()

        with patch(
            'ckanext.versioned_datastore.lib.downloads.download.idownload_implementations',
            return_value=[mock_plugin],
        ):
            with patches.url_for():
                download_details = toolkit.get_action('vds_download_queue')(
                    {},
                    {
                        'query': {'query': {}},
                        'file': {'format': 'csv'},
                        'notifier': {'type': 'none'},
                    },
                )

        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            assert 'resource.json' in archive_files
            assert len(archive_files) == 2

    def test_modify_manifest(self, enqueue_job):
        mock_plugin = ModifyManifestPlugin()

        with patch(
            'ckanext.versioned_datastore.lib.downloads.download.idownload_implementations',
            return_value=[mock_plugin],
        ):
            with patches.url_for():
                download_details = toolkit.get_action('vds_download_queue')(
                    {},
                    {
                        'query': {'query': {}},
                        'file': {'format': 'csv'},
                        'notifier': {'type': 'none'},
                    },
                )

        download_request = DownloadRequest.get(download_details['download_id'])
        assert download_request is not None
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
        matching_zips = [
            f
            for f in os.listdir(download_dir)
            if f.startswith(download_request.derivative_record.download_hash)
        ]
        assert len(matching_zips) == 1
        temp_dir = tempfile.mktemp()
        with zipfile.ZipFile(os.path.join(download_dir, matching_zips[0]), 'r') as zf:
            archive_files = zf.namelist()
            assert 'manifest.json' in archive_files
            assert 'resource.csv' in archive_files
            assert len(archive_files) == 2
            zf.extract('manifest.json', temp_dir)

        with open(os.path.join(temp_dir, 'manifest.json')) as f:
            manifest = json.load(f)
            assert manifest['download_id'] == download_details['download_id']
            assert manifest['totally-new-key'] == 'bananas'

        shutil.rmtree(temp_dir)


class ModifyArgsPlugin:
    def download_before_init(
        self, query_args, derivative_args, server_args, notifier_args
    ):
        derivative_args.format = 'json'
        return query_args, derivative_args, server_args, notifier_args

    def download_modify_manifest(self, manifest, request):
        return manifest

    def download_after_run(self, request):
        return

    def download_after_init(self, request):
        return


class ModifyManifestPlugin:
    def download_before_init(self, *args):
        return args

    def download_modify_manifest(self, manifest, request):
        manifest['totally-new-key'] = 'bananas'
        return manifest

    def download_after_run(self, request):
        return

    def download_after_init(self, request):
        return
