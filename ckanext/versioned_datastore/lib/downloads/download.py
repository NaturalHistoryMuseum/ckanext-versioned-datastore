import hashlib
import json
import os
import os.path
import shutil
from datetime import datetime as dt
from glob import iglob

from ckan.plugins import toolkit
from eevee.indexing.utils import get_elasticsearch_client
from eevee.search import create_version_query
from elasticsearch_dsl import Search
from fastavro import writer

from .loaders import get_derivative_generator, get_file_server, get_notifier
from .query import Query
from .utils import get_schema, calculate_field_counts, filter_data_fields, get_fields
from .. import common
from ..datastore_utils import prefix_resource
from ...model.downloads import CoreFileRecord, DownloadRequest
from ...model.downloads import DerivativeFileRecord


class DownloadRunManager:
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')

    def __init__(self, query_args, derivative_args, server_args, notifier_args):
        self.query = Query.from_query_args(query_args)
        self.derivative_options = derivative_args
        self.server = get_file_server(server_args.type, **server_args.type_args)
        self.notifier = get_notifier(notifier_args.type, **notifier_args.type_args)

        # initialises a log entry in the database
        self.request = DownloadRequest()
        self.request.save()

        # initialise attributes for completing later
        self.derivative_record = None
        self.core_record = None  # will not necessarily be used

    def run(self):
        self.get_derivative()

    @property
    def derivative_hash(self):
        file_options = {
            'format': self.derivative_options.format,
            'format_args': self.derivative_options.format_args,
            'separate_files': self.derivative_options.separate_files,
            'ignore_empty_fields': self.derivative_options.ignore_empty_fields,
            'transform': self.derivative_options.transform
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
            self.derivative_record = DerivativeFileRecord.get_by_filepath(existing_file)
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
            return self.derivative_record

        self.core_record = self.generate_core()

        self.request.update_status(DownloadRequest.state_derivative_gen)
        self.derivative_record = self.generate_derivative()
        return self.derivative_record

    def generate_core(self):
        try:
            download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')

            root_folder = os.path.join(download_dir, self.query.hash)
            record = None
            if os.path.exists(root_folder):
                records = CoreFileRecord.get_by_hash(self.query.hash)
                if records:
                    # use the most recent one
                    record = records[0]
                else:
                    shutil.rmtree(root_folder)
            if record is None:
                os.mkdir(root_folder)
                record = CoreFileRecord(query_hash=self.query.hash,
                                        query=self.query.query,
                                        query_version=self.query.query_version,
                                        resource_ids_and_versions={})

            existing_resources = os.listdir(root_folder)
            resources_to_generate = {rid: v for rid, v in
                                     self.query.resource_ids_and_versions.items() if
                                     f'{rid}_{v}' not in existing_resources}

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
                    fp = os.path.join(root_folder, fn)

                    codec_kwargs = dict(codec='bzip2', codec_compression_level=9)
                    chunk_size = 10000
                    with open(fp, 'wb') as f:
                        writer(f, schema, [], **codec_kwargs)

                    def _flush(record_block):
                        with open(fp, 'a+b') as outfile:
                            writer(outfile, None, record_block, **codec_kwargs)

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
                              total=sum(record.resource_totals.values()),
                              resource_ids_and_versions=self.query.resource_ids_and_versions,
                              field_counts=field_counts)

            record.save()
            return record
        except Exception as e:
            self.request.update_status(DownloadRequest.state_failed, str(e))
            raise e

    def generate_derivative(self):
        start = dt.utcnow()

        derivative_generator = get_derivative_generator(self.derivative_options.format,
                                                        **self.derivative_options.format_args)

        manifest = {
            'download_id': self.request.id,
            'resources': {},
            'separate_files': self.derivative_options.separate_files,
            'file_format': derivative_generator.name,
            'format_args': derivative_generator.format_args,
            'ignore_empty_fields': self.derivative_options.ignore_empty_fields,
            'transform': self.derivative_options.transform,
            'start': start.isoformat(),

            # these get filled in later but we'll initialise them here
            'total_records': 0,
            'files': [],
            'end': None,
            'duration_in_seconds': 0
        }
