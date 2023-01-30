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
import shutil

import fastavro
from ckan.plugins import toolkit, PluginImplementations
from elasticsearch_dsl import Search
from splitgill.indexing.utils import get_elasticsearch_client
from splitgill.search import create_version_query

from .loaders import (
    get_derivative_generator,
    get_file_server,
    get_notifier,
    get_transformation,
)
from .query import Query
from .utils import get_schemas, calculate_field_counts, filter_data_fields, get_fields
from .. import common
from ..datastore_utils import prefix_resource
from ...interfaces import IVersionedDatastoreDownloads
from ...logic.actions.meta.arg_objects import DerivativeArgs
from ...model.downloads import CoreFileRecord, DownloadRequest
from ...model.downloads import DerivativeFileRecord


class DownloadRunManager:
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
    if download_dir is None:
        raise Exception('ckanext.versioned_datastore.download_dir must be set.')
    core_dir = os.path.join(download_dir, 'core')

    def __init__(self, query_args, derivative_args, server_args, notifier_args):
        # allow plugins to make changes to the args
        for plugin in PluginImplementations(IVersionedDatastoreDownloads):
            (
                query_args,
                derivative_args,
                server_args,
                notifier_args,
            ) = plugin.download_before_run(
                query_args, derivative_args, server_args, notifier_args
            )

        self.query = Query.from_query_args(query_args)
        self.derivative_options = derivative_args
        for field, default_value in DerivativeArgs.defaults.items():
            if getattr(self.derivative_options, field) is None:
                setattr(self.derivative_options, field, default_value)
        self.server = get_file_server(server_args.type, **server_args.type_args)

        # initialise core and derivative records
        self.core_record, self.derivative_record = self.check_for_records()

        # initialises a log entry in the database
        self.request = DownloadRequest(
            core_id=self.core_record.id, derivative_id=self.derivative_record.id
        )
        self.request.save()

        self.notifier = get_notifier(
            notifier_args.type, request=self.request, **notifier_args.type_args
        )

        self._temp = []

    @property
    def derivative_hash(self):
        """
        Unique hash for the derivative file options.
        """
        file_options = {
            f: getattr(self.derivative_options, f)
            for f in self.derivative_options.fields
        }
        file_options_hash = hashlib.sha1(json.dumps(file_options).encode('utf-8'))
        return file_options_hash.hexdigest()

    @property
    def hash(self):
        """
        Unique hash for this download request, identified by the query and the
        derivative options.
        """
        to_hash = [self.query.record_hash, self.derivative_hash]
        download_hash = hashlib.sha1('|'.join(to_hash).encode('utf-8'))
        return download_hash.hexdigest()

    @property
    def core_folder_path(self):
        """
        Location of the core files for this query.
        """
        return os.path.join(self.core_dir, self.query.hash)

    def run(self):
        """
        Run the download process.
        """
        self.notifier.notify_start()
        self.request.update_status(DownloadRequest.state_initial)

        try:
            # refresh the db objects because they were probably retrieved in a
            # different session (the __init__ is run by the main ckan process,
            # this is run by the download worker)
            self.request = DownloadRequest.get(self.request.id)
            self.core_record = CoreFileRecord.get(self.core_record.id)
            self.derivative_record = DerivativeFileRecord.get(self.derivative_record.id)

            # generate core file if needed
            self.generate_core()

            # generate derivative file if needed
            self.generate_derivative()

            # finish up
            self.request.update_status(DownloadRequest.state_complete)
            url = self.server.serve(self.request)
            self.notifier.notify_end(url)
        except Exception as e:
            current_state = self.request.state
            self.request.update_status(
                DownloadRequest.state_failed,
                f'{current_state}, {e.__class__.__name__}: {str(e)}',
            )
            self.notifier.notify_error()
            raise e
        finally:
            for t in self._temp:
                try:
                    shutil.rmtree(t)
                except FileNotFoundError:
                    pass
            for plugin in PluginImplementations(IVersionedDatastoreDownloads):
                plugin.download_after_run(self.request)

    def check_for_records(self):
        """
        Check if relevant files and records already exist and returns the records if
        they exist.

        :return: tuple of core_record (or None) and derivative_record (or None)
        """
        # check the download dir exists
        if not os.path.exists(self.download_dir):
            os.mkdir(self.download_dir)

        # also check the core dir exists
        if not os.path.exists(self.core_dir):
            os.mkdir(self.core_dir)

        # initialise empty variables
        core_record = None
        derivative_record = None

        # search for derivative first
        fn = f'*_{self.hash}.zip'
        existing_file = next(iglob(os.path.join(self.download_dir, fn)), None)
        if existing_file is not None:
            # could return multiple options, sorted by most recent first
            possible_records = DerivativeFileRecord.get_by_filepath(existing_file)
            if possible_records:
                # use the most recent one
                derivative_record = possible_records[0]
                core_record = derivative_record.core_record

        # if the core record hasn't been found by searching for the derivative, try and find it now
        if core_record is None:
            if os.path.exists(self.core_folder_path):
                # could return multiple options, sorted by most recent first
                possible_records = CoreFileRecord.get_by_hash(
                    self.query.hash, self.query.resource_hash
                )
                if possible_records:
                    # use the most recent one
                    core_record = possible_records[0]

        if core_record is None:
            core_record = CoreFileRecord(
                query_hash=self.query.hash,
                query=self.query.query,
                query_version=self.query.query_version,
                resource_ids_and_versions=self.query.resource_ids_and_versions,
                resource_hash=self.query.resource_hash,
            )
            core_record.save()

        if derivative_record is None:
            derivative_record = DerivativeFileRecord(
                core_id=core_record.id,
                download_hash=self.hash,
                format=self.derivative_options.format,
                options={
                    f: getattr(self.derivative_options, f)
                    for f in self.derivative_options.fields
                    if f != 'format'
                },
            )
            derivative_record.save()

        return core_record, derivative_record

    def generate_core(self):
        """
        Generates and loads core files.

        This method will add new resources to existing core records
        with identical filters, reducing data duplication.
        :return: the core record
        """
        if not os.path.exists(self.core_folder_path):
            os.makedirs(self.core_folder_path)

        # check if there are new resources to generate
        existing_files = os.listdir(self.core_folder_path)
        resources_to_generate = {
            rid: v
            for rid, v in self.query.resource_ids_and_versions.items()
            if f'{rid}_{v}.avro' not in existing_files
        }

        resource_totals = {k: None for k in self.core_record.resource_ids_and_versions}
        field_counts = {k: None for k in self.core_record.resource_ids_and_versions}

        # get info for resources that weren't just generated first, so we know if they
        # need regenerating
        existing_resources = [
            k
            for k in self.query.resource_ids_and_versions
            if k not in resources_to_generate
        ]
        for resource_id in existing_resources:
            # find a matching core record
            record = CoreFileRecord.find_resource(
                self.query.hash,
                resource_id,
                self.query.resource_ids_and_versions[resource_id],
            )
            if record:
                resource_totals[resource_id] = record.resource_totals[resource_id]
                field_counts[resource_id] = record.field_counts[resource_id]
            else:
                # if there's no record we should regenerate the avro file
                resource_version = self.query.resource_ids_and_versions[resource_id]
                core_file_path = os.path.join(
                    self.core_folder_path, f'{resource_id}_{resource_version}.avro'
                )
                os.remove(core_file_path)
                resources_to_generate[resource_id] = resource_version

        if len(resources_to_generate) > 0:
            es_client = get_elasticsearch_client(
                common.CONFIG,
                sniff_on_start=True,
                sniffer_timeout=60,
                sniff_on_connection_fail=True,
                sniff_timeout=10,
                http_compress=False,
                timeout=30,
            )

            schemas = get_schemas(self.query, es_client)

            for resource_id, version in resources_to_generate.items():
                self.request.update_status(DownloadRequest.state_core_gen, resource_id)
                resource_totals[resource_id] = 0
                field_counts[resource_id] = calculate_field_counts(
                    self.query, es_client, resource_id, version
                )

                search = (
                    Search.from_dict(self.query.translate().to_dict())
                    .index(prefix_resource(resource_id))
                    .using(es_client)
                    .filter(create_version_query(version))
                )

                fn = f'{resource_id}_{version}.avro'
                fp = os.path.join(self.core_folder_path, fn)

                codec_kwargs = dict(codec='bzip2', codec_compression_level=9)
                chunk_size = 10000
                with open(fp, 'wb') as f:
                    fastavro.writer(f, schemas[resource_id], [], **codec_kwargs)

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

        self.core_record.update(
            resource_totals=resource_totals,
            total=sum(resource_totals.values()),
            field_counts=field_counts,
        )

        return self.core_record

    def generate_derivative(self):
        """
        Generates derivative files, if necessary.

        :return:
        """

        self.request.update_status(DownloadRequest.state_derivative_gen)

        if self.derivative_record.filepath is None:
            zip_name = f'{self.request.id}_{self.hash}.zip'
            zip_path = os.path.join(self.download_dir, zip_name)

            self.derivative_record.update(filepath=zip_path)
        else:
            # we don't want to proceed with the rest of the generation
            return self.derivative_record

        # for keeping track of elapsed generation time
        start = dt.utcnow()
        # for storing build files
        temp_dir = tempfile.mkdtemp()
        self._temp.append(temp_dir)

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
            'duration_in_seconds': 0,
        }

        # set up the derivative generators; each generator creates one component.
        # components = individual file groups within the main zip, e.g. one CSV for each
        # resource (multiple components), or multiple files comprising a single DarwinCore
        # archive (single component)
        fields = partial(
            get_fields,
            self.core_record.field_counts,
            self.derivative_options.ignore_empty_fields,
        )
        if self.derivative_options.separate_files:
            components = {
                rid: get_derivative_generator(
                    self.derivative_options.format,
                    output_dir=temp_dir,
                    fields=fields(resource_id=rid),
                    resource_id=rid,
                    query=self.query,
                    **self.derivative_options.format_args,
                )
                for rid in self.query.resource_ids_and_versions
            }
        else:
            gen = get_derivative_generator(
                self.derivative_options.format,
                output_dir=temp_dir,
                fields=fields(),
                query=self.query,
                **self.derivative_options.format_args,
            )
            components = defaultdict(lambda: gen)

        # load transformation functions
        transformations = [
            get_transformation(t, **targs)
            for t, targs in (self.derivative_options.transform or {}).items()
        ]

        for resource_id, version in self.query.resource_ids_and_versions.items():
            # add the resource ID as the message for use in the status page
            self.request.update_status(
                DownloadRequest.state_derivative_gen, resource_id
            )
            if (
                len(self.query.resource_ids_and_versions) > 1
                and self.core_record.resource_totals[resource_id] == 0
            ):
                # don't generate empty files unless there's only one resource
                continue
            derivative_generator = components[resource_id]
            core_file_path = os.path.join(
                self.core_folder_path, f'{resource_id}_{version}.avro'
            )
            with derivative_generator, open(core_file_path, 'rb') as core_file:
                for record in fastavro.reader(core_file):
                    # apply the transformations first
                    for transform in transformations:
                        record = transform(record)
                    # filter out fields that are empty for all records
                    if self.derivative_options.ignore_empty_fields:
                        record = filter_data_fields(
                            record, self.core_record.field_counts[resource_id]
                        )
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

        # allow plugins to make changes
        for plugin in PluginImplementations(IVersionedDatastoreDownloads):
            manifest = plugin.download_modify_manifest(manifest, self.request)

        # write out manifest
        with open(os.path.join(temp_dir, 'manifest.json'), 'w', encoding='utf8') as f:
            json.dump(manifest, f, sort_keys=True, indent=2, ensure_ascii=False)

        # zip everything up
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, True) as z:
            for filename in files_to_zip:
                z.write(os.path.join(temp_dir, filename), arcname=filename)

        return self.derivative_record
