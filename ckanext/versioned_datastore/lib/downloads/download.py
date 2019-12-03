import functools
import hashlib
import io
import json
import shutil
import socket
import tempfile
import zipfile
from datetime import datetime
from glob import iglob
from traceback import format_exception_only

import os
import rq
from ckan import model
from ckan.lib import jobs, mailer
from ckan.plugins import toolkit, PluginImplementations
from eevee.indexing.utils import get_elasticsearch_client
from eevee.search import create_version_query
from elasticsearch_dsl import Search

from .jsonl import jsonl_writer
from .sv import sv_writer
from .utils import calculate_field_counts
from .. import common
from ..datastore_utils import trim_index_name, prefix_resource
from ...interfaces import IVersionedDatastoreDownloads
from ...model.downloads import DatastoreDownload

format_registry = {
    u'csv': functools.partial(sv_writer, dialect=u'excel', extension=u'csv'),
    u'tsv': functools.partial(sv_writer, dialect=u'excel-tab', extension=u'tsv'),
    u'jsonl': jsonl_writer,
}

# TODO: put this in the config/interface so that it can be overridden
default_body = u'''
Hello,

The link to the resource data you requested on https://data.nhm.ac.uk is available at: {url}.
{{extras}}
Best Wishes,
The NHM Data Portal Bot
'''.strip()


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


def queue_download(email_address, download_id, query_hash, query, query_version, search,
                   resource_ids_and_versions, separate_files, file_format, ignore_empty_fields):
    '''
    Queues a job which when run will download the data for the resource.

    :return: the queued job
    '''
    ensure_download_queue_exists()
    request = DownloadRequest(email_address, download_id, query_hash, query, query_version, search,
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

    def __init__(self, email_address, download_id, query_hash, query, query_version, search,
                 resource_ids_and_versions, separate_files, file_format, ignore_empty_fields):
        self.email_address = email_address
        self.download_id = download_id
        self.query_hash = query_hash
        self.query = query
        self.query_version = query_version
        self.search = search
        self.resource_ids_and_versions = resource_ids_and_versions
        self.separate_files = separate_files
        self.file_format = file_format
        self.ignore_empty_fields = ignore_empty_fields

    @property
    def resource_ids(self):
        return sorted(self.resource_ids_and_versions.keys())

    def update_download(self, **kwargs):
        '''
        Update the DatastoreDownload object with the given database id with the given update dict.
        The update dict will be passed directly to SQLAlchemy.

        :param kwargs: a dict of fields to update with the corresponding values
        '''
        download_entry = model.Session.query(DatastoreDownload).get(self.download_id)
        for field, value in kwargs.items():
            setattr(download_entry, field, value)
        download_entry.save()

    def generate_download_hash(self):
        to_hash = [
            self.query_hash,
            self.query_version,
            sorted(self.resource_ids_and_versions.items()),
            self.separate_files,
            self.file_format,
            self.ignore_empty_fields,
        ]
        download_hash = hashlib.sha1(u'|'.join(map(unicode, to_hash)))
        return download_hash.hexdigest()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return unicode(self).encode(u'utf-8')

    def __unicode__(self):
        return u'Download data, id: {}'.format(self.download_id)


def download(request):
    '''
    Downloads the data found by the query in the request and writes it to a zip.

    :param request: the DownloadRequest object, see that for parameter details
    '''
    download_dir = toolkit.config.get(u'ckanext.versioned_datastore.download_dir')
    download_hash = request.generate_download_hash()
    existing_file = next(iglob(os.path.join(download_dir, u'*_{}.zip'.format(download_hash))), None)
    if existing_file is not None:
        previous_download_id = os.path.split(existing_file)[1].split(u'_')[0]
        existing_download = model.Session.query(DatastoreDownload).get(previous_download_id)
        if existing_download is not None:
            request.update_download(state=u'complete', total=existing_download.total,
                                    resource_totals=existing_download.resource_totals)
            zip_name = u'{}_{}.zip'.format(existing_download.id, download_hash)
            send_email(request, zip_name)
            return

    zip_name = u'{}_{}.zip'.format(request.download_id, download_hash)
    zip_path = os.path.join(download_dir, zip_name)

    start = datetime.now()
    request.update_download(state=u'processing')
    target_dir = tempfile.mkdtemp()

    try:
        es_client = get_elasticsearch_client(common.CONFIG, sniff_on_start=True, sniffer_timeout=60,
                                             sniff_on_connection_fail=True, sniff_timeout=10,
                                             http_compress=False)

        # this manifest will be written out as JSON and put in the download zip
        manifest = {
            u'download_id': request.download_id,
            u'resources': {},
            u'separate_files': request.separate_files,
            u'file_format': request.file_format,
            u'ignore_empty_fields': request.ignore_empty_fields,
        }
        # calculate, per resource, the number of values for each field present in the search
        field_counts = calculate_field_counts(request, es_client)
        # choose the writer function based on the requested file format
        writer_function = format_registry[request.file_format]
        # we shouldn't auth anything
        context = {u'ignore_auth': True}
        # keep track of the resource record counts
        resource_counts = {}

        with writer_function(request, target_dir, field_counts) as writer:
            # handle each resource individually. We could search across all resources at the same
            # but we don't need to seeing as we're not doing sorting here. By handling each index
            # one at a time we should be less of an es server burden and also it means we can use
            # the scroll api - if we were to search all resources requested at once we'd probably
            # have to use the multisearch endpoint on es and that doesn't support the scroll api
            for resource_id, version in request.resource_ids_and_versions.items():
                search = Search.from_dict(request.search) \
                    .index(prefix_resource(resource_id)) \
                    .using(es_client) \
                    .filter(create_version_query(version))

                total_records = 0
                for hit in search.scan():
                    data = hit.data.to_dict()
                    resource_id = trim_index_name(hit.meta.index)
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
                resource_counts[resource_id] = total_records

        overall_total = sum(resource_counts.values())
        request.update_download(state=u'zipping', total=overall_total,
                                resource_totals=resource_counts)

        # create a list of files that should be added to the zip, this should include the manifest
        files_to_zip = [u'manifest.json'] + os.listdir(target_dir)

        # add the final data to the manifest
        manifest[u'total_records'] = overall_total
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
        with zipfile.ZipFile(zip_path, u'w', zipfile.ZIP_DEFLATED) as z:
            for filename in files_to_zip:
                z.write(os.path.join(target_dir, filename), arcname=filename)

        # drop the user an email to let them know their download is ready
        try:
            send_email(request, zip_name)
        except (mailer.MailerException, socket.error):
            raise

        request.update_download(state=u'complete')
    except Exception as error:
        error_message = unicode(format_exception_only(type(error), error)[-1].strip())
        request.update_download(state=u'failed', error=error_message)
        raise
    finally:
        # remove the temp dir we were using
        shutil.rmtree(target_dir)


def send_email(request, zip_name):
    '''
    Sends an email to the email address in the passed request informing them that a download has
    completed and providing them with a link to go get it from.

    :param request: the DownloadRequest object
    :param zip_name: the name of the zip file that has been created
    '''
    download_url = u'{}/downloads/{}'.format(toolkit.config.get(u'ckan.site_url'), zip_name)
    # create the default download email body using the url
    body = default_body.format(url=download_url)

    # allow plugins to override the middle section of the email with any additional details
    extras = []
    for plugin in PluginImplementations(IVersionedDatastoreDownloads):
        extra_body = plugin.download_add_to_email_body(request)
        if extra_body:
            extras.append(extra_body)
    if extras:
        body = body.format(extras=u'\n{}\n'.format(u'\n\n'.join(extras)))
    else:
        # this is necessary as it removes the {extras} placeholder in the default body text
        body = body.format(extras=u'')

    mailer.mail_recipient(recipient_email=request.email_address, recipient_name=u'Downloader',
                          subject=u'Data download', body=body)
