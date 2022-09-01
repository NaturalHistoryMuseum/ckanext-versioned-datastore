import hashlib
import json
import os
import os.path
import shutil
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime as dt
from glob import iglob
from functools import partial

import fastavro
from ckan.plugins import toolkit
from eevee.indexing.utils import get_elasticsearch_client
from eevee.search import create_version_query
from elasticsearch_dsl import Search

from .loaders import get_derivative_generator, get_file_server, get_notifier, get_transformation
from .query import Query
from .utils import get_schema, calculate_field_counts, filter_data_fields, get_fields
from .. import common
from ..datastore_utils import prefix_resource
from ...model.downloads import CoreFileRecord, DownloadRequest
from ...model.downloads import DerivativeFileRecord
from ...logic.actions.meta.arg_objects import DerivativeArgs


class DownloadRunManager:
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
    core_dir = os.path.join(download_dir, 'core')

    def __init__(self, query_args, derivative_args, server_args, notifier_args):
        self.query = Query.from_query_args(query_args)
        self.derivative_options = derivative_args
        for field, default_value in DerivativeArgs.defaults.items():
            if getattr(self.derivative_options, field) is None:
                setattr(self.derivative_options, field, default_value)
        self.server = get_file_server(server_args.type, **server_args.type_args)

        # initialises a log entry in the database
        self.request = DownloadRequest()
        self.request.save()

        self.notifier = get_notifier(notifier_args.type, request=self.request, **notifier_args.type_args)

        # initialise attributes for completing later
        self.derivative_record = None
        self.core_record = None  # will not necessarily be used

    def run(self):
        self.notifier.notify_start()
        self.get_derivative()
        url = self.server.serve(self.request)
        self.notifier.notify_end(url)

    @property
    def derivative_hash(self):
        file_options = {
            f: getattr(self.derivative_options, f) for f in self.derivative_options.fields
        }
        file_options_hash = hashlib.sha1(json.dumps(file_options).encode('utf-8'))
        return file_options_hash.hexdigest()

    @property
    def hash(self):
        to_hash = [
            self.query.record_hash,
            self.derivative_hash
        ]
        download_hash = hashlib.sha1('|'.join(to_hash).encode('utf-8'))
        return download_hash.hexdigest()

    @property
    def core_folder_path(self):
        return os.path.join(self.core_dir, self.query.hash)

    def check_for_derivative(self):
        # check the download dir exists
        if not os.path.exists(self.download_dir):
            os.mkdir(self.download_dir)
            # if it doesn't then the file obviously doesn't exist either
            return False

        fn = f'*_{self.hash}.zip'
        existing_file = next(iglob(os.path.join(self.download_dir, fn)), None)
        self.derivative_record = None
        if existing_file is not None:
            possible_records = DerivativeFileRecord.get_by_filepath(existing_file)
            if len(possible_records) > 0:
                self.derivative_record = possible_records[0]
        return self.derivative_record is not None

    def get_derivative(self):
        '''
        Find or create a derivative file and return the associated database entry (a DerivativeFileRecord instance).
        :return:
        '''
        # does derivative exist?
        derivative_exists = self.check_for_derivative()

        if derivative_exists:
            self.request.update_status(DownloadRequest.state_retrieving)
        else:
            self.core_record = self.generate_core()
            self.derivative_record = self.generate_derivative()
        self.request.derivative_id = self.derivative_record.id
        return self.derivative_record

    def generate_core(self):
        try:
            if not os.path.exists(self.core_dir):
                os.mkdir(self.core_dir)
            record = None
            if os.path.exists(self.core_folder_path):
                records = CoreFileRecord.get_by_hash(self.query.hash)
                if records:
                    # use the most recent one
                    record = records[0]
                else:
                    shutil.rmtree(self.core_folder_path)
            if record is None:
                os.mkdir(self.core_folder_path)
                record = CoreFileRecord(query_hash=self.query.hash,
                                        query=self.query.query,
                                        query_version=self.query.query_version,
                                        resource_ids_and_versions={})

            existing_resources = os.listdir(self.core_folder_path)
            resources_to_generate = {rid: v for rid, v in
                                     self.query.resource_ids_and_versions.items() if
                                     f'{rid}_{v}.avro' not in existing_resources}

            if len(resources_to_generate) > 0:
                es_client = get_elasticsearch_client(common.CONFIG, sniff_on_start=True,
                                                     sniffer_timeout=60,
                                                     sniff_on_connection_fail=True,
                                                     sniff_timeout=10,
                                                     http_compress=False, timeout=30)

                schema = get_schema(self.query, es_client)
                resource_totals = {k: v for k, v in (record.resource_totals or {}).items()}
                field_counts = {k: v for k, v in (record.field_counts or {}).items()}

                for resource_id, version in resources_to_generate.items():
                    self.request.update_status(DownloadRequest.state_core_gen,
                                               f'Generating {resource_id}')
                    resource_totals[resource_id] = 0
                    field_counts[resource_id] = calculate_field_counts(self.query, es_client,
                                                                       resource_id, version)

                    search = Search.from_dict(self.query.translate().to_dict()) \
                        .index(prefix_resource(resource_id)) \
                        .using(es_client) \
                        .filter(create_version_query(version))

                    fn = f'{resource_id}_{version}.avro'
                    fp = os.path.join(self.core_folder_path, fn)

                    codec_kwargs = dict(codec='bzip2', codec_compression_level=9)
                    chunk_size = 10000
                    with open(fp, 'wb') as f:
                        fastavro.writer(f, schema, [], **codec_kwargs)

                    def _flush(record_block):
                        with open(fp, 'a+b') as outfile:
                            fastavro.writer(outfile, None, record_block, **codec_kwargs)

                    chunk = []
                    for hit in search.scan():
                        data = hit.data.to_dict()
                        resource_totals[resource_id] += 1
                        chunk.append(data)
                        if len(chunk) == chunk_size:
                            _flush(chunk)
                            chunk = []
                    _flush(chunk)

                # use .update() so it updates the modified datetime
                record.update(resource_totals=resource_totals,
                              total=sum(resource_totals.values()),
                              resource_ids_and_versions=self.query.resource_ids_and_versions,
                              field_counts=field_counts)

            record.save()
            return record
        except Exception as e:
            self.request.update_status(DownloadRequest.state_failed, str(e))
            self.notifier.notify_error()
            raise e

    def generate_derivative(self):
        start = dt.utcnow()
        self.request.update_status(DownloadRequest.state_derivative_gen, 'initialising')

        try:
            temp_dir = tempfile.mkdtemp()
            zip_name = f'{self.request.id}_{self.hash}.zip'
            zip_path = os.path.join(self.download_dir, zip_name)

            derivative_record = DerivativeFileRecord(core_id=self.core_record.id,
                                                     download_hash=self.hash,
                                                     format=self.derivative_options.format,
                                                     options={f: getattr(self.derivative_options, f)
                                                              for
                                                              f in self.derivative_options.fields if
                                                              f != 'format'},
                                                     filepath=zip_path)

            manifest = {
                'download_id': self.request.id,
                'resources': {},
                'separate_files': self.derivative_options.separate_files,
                'file_format': self.derivative_options.format,
                'format_args': self.derivative_options.format_args,
                'ignore_empty_fields': self.derivative_options.ignore_empty_fields,
                'transform': self.derivative_options.transform,
                'total_records': self.core_record.total,
                'start': start.isoformat(),

                # these get filled in later but we'll initialise them here
                'files': [],
                'end': None,
                'duration_in_seconds': 0
            }

            # components = individual file groups within the main zip, e.g. one CSV for each
            # resource (multiple components), or multiple files comprising a single DarwinCore
            # archive (single component)
            fields = partial(get_fields, self.core_record.field_counts,
                             self.derivative_options.ignore_empty_fields)
            if self.derivative_options.separate_files:
                components = {rid: get_derivative_generator(self.derivative_options.format,
                                                            output_dir=temp_dir,
                                                            fields=fields(resource_id=rid),
                                                            resource_id=rid,
                                                            **self.derivative_options.format_args)
                              for rid in self.query.resource_ids_and_versions}
            else:
                gen = get_derivative_generator(self.derivative_options.format,
                                               output_dir=temp_dir,
                                               fields=fields(),
                                               **self.derivative_options.format_args)
                components = defaultdict(lambda: gen)

            transformations = {t: get_transformation(t, **targs) for
                               t, targs in (self.derivative_options.transform or {}).items()}

            for resource_id, version in self.query.resource_ids_and_versions.items():
                self.request.update_status(DownloadRequest.state_derivative_gen, resource_id)
                derivative_generator = components[resource_id]
                core_file_path = os.path.join(self.core_folder_path, f'{resource_id}_{version}.avro')
                with derivative_generator, open(core_file_path, 'rb') as core_file:
                    for record in fastavro.reader(core_file):
                        for transform in transformations:
                            record = transform(record)
                        if self.derivative_options.ignore_empty_fields:
                            record = filter_data_fields(record,
                                                        self.core_record.field_counts[resource_id])
                        derivative_generator.write(record)
            end = dt.utcnow()
            manifest['end'] = end.isoformat()

            duration = (end - start).total_seconds()
            manifest['duration_in_seconds'] = duration

            files_to_zip = os.listdir(temp_dir) + ['manifest.json']
            manifest['files'] = files_to_zip

            self.request.update_status(DownloadRequest.state_packaging)

            # write out manifest
            with open(os.path.join(temp_dir, 'manifest.json'), 'w', encoding='utf8') as f:
                json.dump(manifest, f, sort_keys=True, indent=2, ensure_ascii=False)

            # zip everything up
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, True) as z:
                for filename in files_to_zip:
                    z.write(os.path.join(temp_dir, filename), arcname=filename)

            self.request.update_status(DownloadRequest.state_complete)

            derivative_record.save()
            self.request.derivative_id = derivative_record.id
            return derivative_record
        except Exception as e:
            self.request.update_status(DownloadRequest.state_failed, str(e))
            self.notifier.notify_error()
            raise e
