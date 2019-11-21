import functools
import io
import json
import shutil
import socket
import tempfile
import zipfile

import os
import rq
from ckan.lib import jobs, mailer
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib import utils
from ckanext.versioned_datastore.lib.downloads.jsonl import jsonl_writer
from ckanext.versioned_datastore.lib.downloads.sv import sv_writer
from ckanext.versioned_datastore.lib.downloads.utils import calculate_field_counts
from ckanext.versioned_datastore.lib.query import generate_query_hash
from datetime import datetime
from eevee.indexing.utils import get_elasticsearch_client
from eevee.search import create_version_query
from elasticsearch_dsl import Search

format_registry = {
    u'csv': functools.partial(sv_writer, dialect=u'excel', extension=u'csv'),
    u'tsv': functools.partial(sv_writer, dialect=u'excel-tab', extension=u'tsv'),
    u'jsonl': jsonl_writer,
}


def ensure_download_queue_exists():
    '''
    This is a hack to get around the lack of rq Queue kwarg exposure from ckanext-rq. The default
    timeout for queues is 180 seconds in rq which is not long enough for our download tasks but the
    timeout parameter hasn't been exposed. This code creates a new queue in the ckanext-rq cache so
    that when enqueuing new jobs it is used rather than a default one. Once this bug has been fixed
    in ckan/ckanext-rq this code will be removed.

    The queue is only added if not already in existence so this is safe to call multiple times.
    '''
    name = jobs.add_queue_name_prefix(u'download')
    if name not in jobs._queues:
        # set the timeout to 12 hours
        queue = rq.Queue(name, default_timeout=60 * 60 * 12, connection=jobs._connect())
        # add the queue to the queue cache
        jobs._queues[name] = queue


def queue_download(email_address, query, query_version, search, resource_ids_and_versions,
                   separate_files, file_format, ignore_empty_fields):
    '''
    Queues a job which when run will download the data for the resource.

    :return: the queued job
    '''
    ensure_download_queue_exists()
    request = DownloadRequest(email_address, query, query_version, search,
                              resource_ids_and_versions, separate_files, file_format,
                              ignore_empty_fields)
    return toolkit.enqueue_job(download, args=[request], queue=u'download', title=unicode(request))


class DownloadRequest(object):
    '''
    Class representing a request to download data. We use a class like this for two reasons, firstly
    to avoid having a long list of arguments passed through to queued functions, and secondly
    because rq by default logs the arguments sent to a function and if the records argument is a
    large list of dicts this becomes insane.
    '''

    def __init__(self, email_address, query, query_version, search, resource_ids_and_versions,
                 separate_files, file_format, ignore_empty_fields):
        self.email_address = email_address
        self.query = query
        self.query_version = query_version
        self.search = search
        self.resource_ids_and_versions = resource_ids_and_versions
        self.separate_files = separate_files
        self.file_format = file_format
        self.ignore_empty_fields = ignore_empty_fields

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return unicode(self).encode(u'utf-8')

    def __unicode__(self):
        return u'Download data from {} resources'.format(len(self.resource_ids_and_versions))

    def generate_query_hash(self):
        return generate_query_hash(self.query, self.query_version, None,
                                   list(self.resource_ids_and_versions.keys()),
                                   self.resource_ids_and_versions)


def download(request):
    '''
    Downloads the data found by the query in the request and writes it to a zip.

    :param request: the DownloadRequest object, see that for parameter details
    '''
    es_client = get_elasticsearch_client(utils.CONFIG, sniff_on_start=True, sniffer_timeout=60,
                                         sniff_on_connection_fail=True, sniff_timeout=10,
                                         http_compress=False)
    target_dir = tempfile.mkdtemp()

    try:
        start = datetime.now()
        # this manifest will be written out as JSON and put in the download zip
        manifest = {
            u'resources': {},
        }
        # calculate, per resource, the number of values for each field present in the search
        field_counts = calculate_field_counts(request, es_client)
        # choose the writer function based on the requested file format
        writer_function = format_registry[request.file_format]
        # we shouldn't auth anything
        context = {u'ignore_auth': True}

        with writer_function(request, target_dir, field_counts) as writer:
            # handle each resource individually. We could search across all resources at the same
            # but we don't need to seeing as we're not doing sorting here. By handling each index
            # one at a time we should be less of an es server burden and also it means we can use
            # the scroll api - if we were to search all resources requested at once we'd probably
            # have to use the multisearch endpoint on es and that doesn't support the scroll api
            for resource_id, version in request.resource_ids_and_versions.items():
                search = Search.from_dict(request.search) \
                    .index(utils.prefix_resource(resource_id)) \
                    .using(es_client) \
                    .filter(create_version_query(version))

                total_records = 0
                for hit in search.scan():
                    data = hit.data.to_dict()
                    resource_id = utils.trim_index_name(hit.meta.index)
                    # call the write function returned by our format specific writer context manager
                    writer(hit, data, resource_id)
                    total_records += 1

                # if no records were written, move on and don't populate the manifest
                if total_records == 0:
                    continue

                # retrieve information about the resource and package and add it to the manifest
                resource_dict = toolkit.get_action(u'resource_show')(context, {u'id': resource_id})
                package_id = resource_dict[u'package_id']
                package_dict = toolkit.get_action(u'package_show')(context, {u'id': package_id})
                manifest[u'resources'][resource_id] = {
                    u'name': resource_dict[u'name'],
                    u'package_id': package_id,
                    u'package_title': package_dict[u'title'],
                    u'total_records': total_records,
                    u'field_counts': field_counts[resource_id],
                    u'version': version,
                }

        # create a list of files that should be added to the zip, this should include the manifest
        files_to_zip = [u'manifest.json'] + os.listdir(target_dir)

        # add the final data to the manifest
        manifest[u'files'] = files_to_zip
        end = datetime.now()
        manifest[u'start'] = start.isoformat()
        manifest[u'end'] = end.isoformat()
        manifest[u'duration_in_seconds'] = (end - start).total_seconds()

        # write the manifest out
        with io.open(os.path.join(target_dir, u'manifest.json'), u'w', encoding=u'utf8') as f:
            data = json.dumps(manifest, f, sort_keys=True, indent=2, ensure_ascii=False)
            f.write(unicode(data))

        # zip up the files into the downloads directory
        download_dir = toolkit.config.get(u'ckanext.versioned_datastore.download_dir')
        zip_name = u'{}.zip'.format(request.generate_query_id())
        zip_path = os.path.join(download_dir, zip_name)
        # remove any existing zip under this name
        if os.path.exists(zip_path):
            os.remove(zip_path)
        with zipfile.ZipFile(zip_path, u'w', zipfile.ZIP_DEFLATED) as z:
            for filename in files_to_zip:
                z.write(os.path.join(target_dir, filename), arcname=filename)

        # drop the user an email to let them know their download is ready
        try:
            send_email(request.email_address, zip_name)
        except (mailer.MailerException, socket.error):
            raise

    finally:
        # remove the temp dir we were using
        shutil.rmtree(target_dir)


def send_email(email_address, zip_name):
    download_url = u'{}/downloads/{}'.format(toolkit.config.get(u'ckan.site_url'), zip_name)
    body = u'Email! Zip: {}'.format(download_url)
    mail_dict = {
        u'recipient_email': email_address,
        u'recipient_name': u'Downloader',
        u'subject': u'Data download',
        u'body': body,
        # u'headers': {
        #     u'reply-to': toolkit.config.get(u'smtp.mail_from')
        # }
    }

    mailer.mail_recipient(**mail_dict)
