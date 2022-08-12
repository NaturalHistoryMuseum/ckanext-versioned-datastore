import json
import os.path

from ckan.plugins import toolkit
from eevee.indexing.utils import get_elasticsearch_client
from eevee.search import create_version_query
from elasticsearch_dsl import Search
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from .utils import get_schema
from .. import common
from ..datastore_utils import prefix_resource
from ...model.downloads import CoreFileRecord, DownloadRequest


def generate_core(query, request: DownloadRequest):
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')

    try:
        es_client = get_elasticsearch_client(common.CONFIG, sniff_on_start=True, sniffer_timeout=60,
                                             sniff_on_connection_fail=True, sniff_timeout=10,
                                             http_compress=False, timeout=30)

        schema = get_schema(query, es_client)

        output_folder_name = f'{request.id}_{query.hash}'
        output_folder_path = os.path.join(download_dir, output_folder_name)

        # this root folder holds a parquet file for each resource - together this is the dataset
        os.mkdir(output_folder_path)

        resource_totals = {}

        for resource_id, version in query.resource_ids_and_versions.items():
            resource_totals[resource_id] = 0

            search = Search.from_dict(query.translate().to_dict()) \
                .index(prefix_resource(resource_id)) \
                .using(es_client) \
                .filter(create_version_query(version))

            fn = f'{resource_id}.avro'
            fp = os.path.join(output_folder_path, fn)

            writer = DataFileWriter(open(fp, 'wb'), DatumWriter(), schema, codec='bzip2')

            for hit in search.scan():
                data = hit.data.to_dict()
                writer.append(data)
                resource_totals[resource_id] += 1

            writer.close()

        total_rows = sum(resource_totals.values())

        core_record = CoreFileRecord(query_hash=query.hash,
                                     query=query.query,
                                     query_version=query.query_version,
                                     resource_ids_and_versions=query.resource_ids_and_versions,
                                     total=total_rows,
                                     resource_totals=resource_totals,
                                     filename=output_folder_name)
        core_record.save()
        return core_record
    except Exception as e:
        request.update_status(request.state_failed)
        request.update(message=str(e))
        raise e
