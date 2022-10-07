import hashlib
import json
import os
import os.path
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime as dt
from functools import partial
from glob import iglob

import fastavro
from eevee.indexing.utils import get_elasticsearch_client
from eevee.search import create_version_query
from elasticsearch_dsl import Search

from ckan.plugins import toolkit
from .loaders import get_derivative_generator, get_file_server, get_notifier, get_transformation
from .query import Query
from .utils import get_schema, calculate_field_counts, filter_data_fields, get_fields
from .. import common
from ..datastore_utils import prefix_resource
from ...logic.actions.meta.arg_objects import DerivativeArgs
from ...model.downloads import CoreFileRecord, DownloadRequest
from ...model.downloads import DerivativeFileRecord


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

        self.notifier = get_notifier(notifier_args.type, request=self.request,
                                     **notifier_args.type_args)

        # initialise attributes for completing later
        self.derivative_record = None
        self.core_record = None

    @property
    def derivative_hash(self):
        '''
        Unique hash for the derivative file options.
        '''
        file_options = {
            f: getattr(self.derivative_options, f) for f in self.derivative_options.fields
        }
        file_options_hash = hashlib.sha1(json.dumps(file_options).encode('utf-8'))
        return file_options_hash.hexdigest()

    @property
    def hash(self):
        '''
        Unique hash for this download request, identified by the query and the derivative options.
        '''
        to_hash = [
            self.query.record_hash,
            self.derivative_hash
        ]
        download_hash = hashlib.sha1('|'.join(to_hash).encode('utf-8'))
        return download_hash.hexdigest()

    @property
    def core_folder_path(self):
        '''
        Location of the core files for this query.
        '''
        return os.path.join(self.core_dir, self.query.hash)

    def run(self):
        '''
        Run the download process.
        '''
        self.notifier.notify_start()
        self.request.update_status(DownloadRequest.state_initial)

        try:
            # load records, if they exist
            self.check_for_records()

            # generate core file if needed
            self.generate_core()

            # generate derivative file if needed
            self.generate_derivative()

            # finish up
            self.request.update_status(DownloadRequest.state_complete)
            url = self.server.serve(self.request)
            self.notifier.notify_end(url)
        except Exception as e:
            self.request.update_status(DownloadRequest.state_failed, str(e))
            self.notifier.notify_error()
            raise e

    def check_for_records(self):
        '''
        Check if relevant files and records already exist and returns the records if they exist.
        :return: tuple of core_record (or None) and derivative_record (or None)
        '''
        # check the download dir exists
        if not os.path.exists(self.download_dir):
            os.mkdir(self.download_dir)
            # if it doesn't then the file obviously doesn't exist either
            return False

        # also check the core dir exists
        if not os.path.exists(self.core_dir):
            os.mkdir(self.core_dir)

        # initialise empty variables
        self.core_record = None
        self.derivative_record = None

        # search for derivative first
        fn = f'*_{self.hash}.zip'
        existing_file = next(iglob(os.path.join(self.download_dir, fn)), None)
        if existing_file is not None:
            # could return multiple options, sorted by most recent first
            possible_records = DerivativeFileRecord.get_by_filepath(existing_file)
            if possible_records:
                # use the most recent one
                self.derivative_record = possible_records[0]
                self.core_record = self.derivative_record.core_record
                # we want to update the request with the IDs ASAP in case of errors
                self.request.update(core_id=self.core_record.id,
                                    derivative_id=self.derivative_record.id)

        # if the core record hasn't been found by searching for the derivative, try and find it now
        if self.core_record is None:
            if os.path.exists(self.core_folder_path):
                # could return multiple options, sorted by most recent first
                possible_records = CoreFileRecord.get_by_hash(self.query.hash,
                                                              self.query.resource_hash)
                if possible_records:
                    # use the most recent one
                    self.core_record = possible_records[0]
                    # update core_id ASAP in case of errors
                    self.request.update(core_id=self.core_record.id)

        # these don't really need to be returned but we may as well
        return self.core_record, self.derivative_record

    def generate_core(self):
        '''
        Generates and loads core files. This method will add new resources to existing core records
        with identical filters, reducing data duplication.
        :return: the core record
        '''
        if self.core_record is None:
            record = CoreFileRecord(query_hash=self.query.hash,
                                    query=self.query.query,
                                    query_version=self.query.query_version,
                                    resource_ids_and_versions=self.query.resource_ids_and_versions,
                                    resource_hash=self.query.resource_hash)
            record.save()
            self.core_record = record
        # if self.core_record was already set then core_id should already be set, but just in case:
        self.request.update(core_id=self.core_record.id)

        if not os.path.exists(self.core_folder_path):
            os.mkdir(self.core_folder_path)

        # check if there are new resources to generate
        existing_files = os.listdir(self.core_folder_path)
        resources_to_generate = {rid: v for rid, v in
                                 self.query.resource_ids_and_versions.items() if
                                 f'{rid}_{v}.avro' not in existing_files}

        resource_totals = {k: None for k in self.core_record.resource_ids_and_versions}
        field_counts = {k: None for k in self.core_record.resource_ids_and_versions}

        if len(resources_to_generate) > 0:
            es_client = get_elasticsearch_client(common.CONFIG, sniff_on_start=True,
                                                 sniffer_timeout=60,
                                                 sniff_on_connection_fail=True,
                                                 sniff_timeout=10,
                                                 http_compress=False, timeout=30)

            schema = get_schema(self.query, es_client)

            for resource_id, version in resources_to_generate.items():
                self.request.update_status(DownloadRequest.state_core_gen,
                                           resource_id)
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

        # now get info for resources that weren't just generated
        existing_resources = [k for k, v in resource_totals.items() if v is None]
        for resource_id in existing_resources:
            # find a matching core record
            record = CoreFileRecord.find_resource(self.query.hash, resource_id,
                                                  self.query.resource_ids_and_versions[resource_id])
            if record:
                resource_totals[resource_id] = record.resource_totals[resource_id]
                field_counts[resource_id] = record.field_counts[resource_id]

        self.core_record.update(resource_totals=resource_totals,
                                total=sum(resource_totals.values()),
                                field_counts=field_counts)

        return self.core_record

    def generate_derivative(self):
        '''
        Generates derivative files, if necessary.
        :return:
        '''

        if self.derivative_record is None:
            zip_name = f'{self.request.id}_{self.hash}.zip'
            zip_path = os.path.join(self.download_dir, zip_name)

            record = DerivativeFileRecord(core_id=self.core_record.id,
                                          download_hash=self.hash,
                                          format=self.derivative_options.format,
                                          options={f: getattr(self.derivative_options, f) for f in
                                                   self.derivative_options.fields if f != 'format'},
                                          filepath=zip_path)
            record.save()
            self.derivative_record = record
            self.request.update(derivative_id=self.derivative_record.id)
        else:
            # if self.derivative_record was already set then derivative_id should already be set,
            # but just in case:
            self.request.update(derivative_id=self.derivative_record.id)
            # we don't want to proceed with the rest of the generation
            return self.derivative_record

        # for keeping track of elapsed generation time
        start = dt.utcnow()
        # for storing build files
        temp_dir = tempfile.mkdtemp()

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

        # set up the derivative generators; each generator creates one component.
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
                                                        query=self.query,
                                                        **self.derivative_options.format_args)
                          for rid in self.query.resource_ids_and_versions}
        else:
            gen = get_derivative_generator(self.derivative_options.format,
                                           output_dir=temp_dir,
                                           fields=fields(),
                                           query=self.query,
                                           **self.derivative_options.format_args)
            components = defaultdict(lambda: gen)

        # load transformation functions
        transformations = {t: get_transformation(t, **targs) for
                           t, targs in (self.derivative_options.transform or {}).items()}

        for resource_id, version in self.query.resource_ids_and_versions.items():
            # add the resource ID as the message for use in the status page
            self.request.update_status(DownloadRequest.state_derivative_gen, resource_id)
            derivative_generator = components[resource_id]
            core_file_path = os.path.join(self.core_folder_path,
                                          f'{resource_id}_{version}.avro')
            with derivative_generator, open(core_file_path, 'rb') as core_file:
                for record in fastavro.reader(core_file):
                    # apply the transformations first
                    for transform in transformations:
                        record = transform(record)
                    # filter out fields that are empty for all records
                    if self.derivative_options.ignore_empty_fields:
                        record = filter_data_fields(record,
                                                    self.core_record.field_counts[resource_id])
                    # then write the record
                    derivative_generator.write(record)

        if self.derivative_options.separate_files:
            for generator in components.values():
                generator.cleanup()
        else:
            components.default_factory().cleanup()

        # generation finished
        self.request.update_status(DownloadRequest.state_packaging)

        end = dt.utcnow()
        manifest['end'] = end.isoformat()

        duration = (end - start).total_seconds()
        manifest['duration_in_seconds'] = duration

        files_to_zip = os.listdir(temp_dir) + ['manifest.json']
        manifest['files'] = files_to_zip

        # write out manifest
        with open(os.path.join(temp_dir, 'manifest.json'), 'w', encoding='utf8') as f:
            json.dump(manifest, f, sort_keys=True, indent=2, ensure_ascii=False)

        # zip everything up
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, True) as z:
            for filename in files_to_zip:
                z.write(os.path.join(temp_dir, filename), arcname=filename)

        return self.derivative_record
