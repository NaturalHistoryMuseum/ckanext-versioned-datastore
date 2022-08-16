import os.path

from ckan.plugins import toolkit
from eevee.indexing.utils import get_elasticsearch_client
from eevee.search import create_version_query
from elasticsearch_dsl import Search
from fastavro import writer

from .utils import get_schema
from .. import common
from ..datastore_utils import prefix_resource
from ...model.downloads import CoreFileRecord, DownloadRequest


def generate_core(query, request: DownloadRequest):
    try:
        download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')

        root_folder = os.path.join(download_dir, query.hash)
        if os.path.exists(root_folder):
            record = CoreFileRecord.get_by_hash(query.hash)
        else:
            os.mkdir(root_folder)
            record = CoreFileRecord(query_hash=query.hash,
                                    query=query.query,
                                    query_version=query.query_version,
                                    resource_ids_and_versions={})

        existing_resources = os.listdir(root_folder)
        resources_to_generate = {rid: v for rid, v in query.resource_ids_and_versions.items() if
                                 f'{rid}_{v}' not in existing_resources}

        es_client = get_elasticsearch_client(common.CONFIG, sniff_on_start=True, sniffer_timeout=60,
                                             sniff_on_connection_fail=True, sniff_timeout=10,
                                             http_compress=False, timeout=30)

        schema = get_schema(query, es_client)

        for resource_id, version in resources_to_generate.items():
            request.update_status(DownloadRequest.state_core_gen, f'Generating {resource_id}')
            record.resource_totals[resource_id] = 0

            search = Search.from_dict(query.translate().to_dict()) \
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
                record.resource_totals[resource_id] += 1
                chunk.append(data)
                if len(chunk) == chunk_size:
                    _flush(chunk)
                    chunk = []
            _flush(chunk)

        record.total = sum(record.resource_totals.values())
        record.resource_ids_and_versions = query.resource_ids_and_versions
        record.save()
        return record
    except Exception as e:
        request.update_status(request.state_failed, str(e))
        raise e
