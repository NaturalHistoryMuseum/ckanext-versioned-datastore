import functools
import hashlib
import json
import os
import shutil
import socket
import tempfile
import zipfile
from ckan import model
from ckan.lib import mailer
from ckan.plugins import toolkit, PluginImplementations
from datetime import datetime
from splitgill.indexing.utils import get_elasticsearch_client
from splitgill.search import create_version_query
from elasticsearch_dsl import Search
from glob import iglob
from jinja2 import Template
from traceback import format_exception_only

from .dwc import dwc_writer
from .jsonl import jsonl_writer
from .sv import sv_writer
from .xlsx import xlsx_writer
from .utils import calculate_field_counts
from .transform import Transform
from .. import common
from ..datastore_utils import trim_index_name, prefix_resource
from ...interfaces import IVersionedDatastoreDownloads
from ...model.downloads import (
    DatastoreDownload,
    state_complete,
    state_failed,
    state_processing,
    state_zipping,
)

format_registry = {
    'csv': functools.partial(sv_writer, dialect='excel', extension='csv'),
    'tsv': functools.partial(sv_writer, dialect='excel-tab', extension='tsv'),
    'jsonl': jsonl_writer,
    'dwc': dwc_writer,
    'xlsx': xlsx_writer,
}

default_body = '''
Hello,
The link to the resource data you requested on {{ site_url }} is available at {{ download_url }}.

Best Wishes,
The Download Bot
'''.strip()

default_html_body = '''
<html lang="en">
<body>
<p>Hello,</p>
<p>The link to the resource data you requested on <a href="{{ site_url }}">{{ site_url }}</a> is
available at <a href="{{ download_url }}">here</a>.</p>
<br />
<p>Best Wishes,</p>
<p>The Download Bot</p>
</body>
</html>
'''.strip()


def queue_download(
    email_address,
    download_id,
    query_hash,
    query,
    query_version,
    search,
    resource_ids_and_versions,
    separate_files,
    file_format,
    format_args,
    ignore_empty_fields,
    transform,
):
    """
    Queues a job which when run will download the data for the resource.

    :return: the queued job
    """
    request = DownloadRequest(
        email_address,
        download_id,
        query_hash,
        query,
        query_version,
        search,
        resource_ids_and_versions,
        separate_files,
        file_format,
        format_args,
        ignore_empty_fields,
        transform,
    )
    # pass a timeout of 1 hour (3600 seconds)
    return toolkit.enqueue_job(
        download,
        args=[request],
        queue='download',
        title=str(request),
        rq_kwargs={'timeout': 3600},
    )


class DownloadRequest(object):
    """
    Class representing a request to download data.

    We use a class like this for two reasons, firstly to avoid having a long list of
    arguments passed through to queued functions, and secondly because rq by default
    logs the arguments sent to a function and if the records argument is a large list of
    dicts this becomes insane.
    """

    def __init__(
        self,
        email_address,
        download_id,
        query_hash,
        query,
        query_version,
        search,
        resource_ids_and_versions,
        separate_files,
        file_format,
        format_args,
        ignore_empty_fields,
        transform,
    ):
        self.email_address = email_address
        self.download_id = download_id
        self.query_hash = query_hash
        self.query = query
        self.query_version = query_version
        self.search = search
        self.resource_ids_and_versions = resource_ids_and_versions
        self.separate_files = separate_files
        self.file_format = file_format
        self.format_args = format_args
        self.ignore_empty_fields = ignore_empty_fields
        self.transform = transform

    @property
    def resource_ids(self):
        return sorted(self.resource_ids_and_versions.keys())

    def update_download(self, **kwargs):
        """
        Update the DatastoreDownload object with the given database id with the given
        update dict. The update dict will be passed directly to SQLAlchemy.

        :param kwargs: a dict of fields to update with the corresponding values
        """
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
            self.format_args,
            self.ignore_empty_fields,
            self.transform,
        ]
        download_hash = hashlib.sha1('|'.join(map(str, to_hash)).encode('utf-8'))
        return download_hash.hexdigest()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'Download data, id: {self.download_id}'


def download(request):
    """
    Downloads the data found by the query in the request and writes it to a zip.

    :param request: the DownloadRequest object, see that for parameter details
    """
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
    # make sure this download dir exists
    if not os.path.exists(download_dir):
        os.mkdir(download_dir)
    download_hash = request.generate_download_hash()
    existing_file = next(
        iglob(os.path.join(download_dir, f'*_{download_hash}.zip')), None
    )
    if existing_file is not None:
        previous_download_id = os.path.split(existing_file)[1].split('_')[0]
        existing_download = model.Session.query(DatastoreDownload).get(
            previous_download_id
        )
        if existing_download is not None:
            request.update_download(
                state='complete',
                total=existing_download.total,
                resource_totals=existing_download.resource_totals,
            )
            zip_name = f'{existing_download.id}_{download_hash}.zip'
            send_email(request, zip_name)
            return

    zip_name = f'{request.download_id}_{download_hash}.zip'
    zip_path = os.path.join(download_dir, zip_name)

    start = datetime.now()
    request.update_download(state=state_processing)
    target_dir = tempfile.mkdtemp()

    try:
        es_client = get_elasticsearch_client(
            common.CONFIG,
            sniff_on_start=True,
            sniffer_timeout=60,
            sniff_on_connection_fail=True,
            sniff_timeout=10,
            http_compress=False,
            timeout=30,
        )

        for plugin in PluginImplementations(IVersionedDatastoreDownloads):
            request = plugin.download_before_write(request)

        # this manifest will be written out as JSON and put in the download zip
        manifest = {
            'download_id': request.download_id,
            'resources': {},
            'separate_files': request.separate_files,
            'file_format': request.file_format,
            'format_args': request.format_args,
            'ignore_empty_fields': request.ignore_empty_fields,
            'transform': request.transform,
        }
        # calculate, per resource, the number of values for each field present in the search
        field_counts = calculate_field_counts(request, es_client)
        # choose the writer function based on the requested file format
        writer_function = format_registry[request.file_format]
        # we shouldn't auth anything
        context = {'ignore_auth': True}
        # keep track of the resource record counts
        resource_counts = {}

        with writer_function(request, target_dir, field_counts) as writer:
            # handle each resource individually. We could search across all resources at the same
            # but we don't need to seeing as we're not doing sorting here. By handling each index
            # one at a time we should be less of an es server burden and also it means we can use
            # the scroll api - if we were to search all resources requested at once we'd probably
            # have to use the multisearch endpoint on es and that doesn't support the scroll api
            for resource_id, version in request.resource_ids_and_versions.items():
                search = (
                    Search.from_dict(request.search)
                    .index(prefix_resource(resource_id))
                    .using(es_client)
                    .filter(create_version_query(version))
                )

                total_records = 0
                for hit in search.scan():
                    data = hit.data.to_dict()
                    data = Transform.transform_data(data, request.transform)
                    resource_id = trim_index_name(hit.meta.index)
                    # call the write function returned by our format specific writer context manager
                    writer(hit, data, resource_id)
                    total_records += 1

                # if no records were written, move on and don't populate the manifest
                if total_records == 0:
                    continue

                # retrieve information about the resource and package and add it to the manifest
                resource_dict = toolkit.get_action('resource_show')(
                    context, {'id': resource_id}
                )
                package_id = resource_dict['package_id']
                package_dict = toolkit.get_action('package_show')(
                    context, {'id': package_id}
                )
                manifest['resources'][resource_id] = {
                    'name': resource_dict['name'],
                    'package_id': package_id,
                    'package_title': package_dict['title'],
                    'total_records': total_records,
                    'field_counts': field_counts[resource_id],
                    'version': version,
                }
                resource_counts[resource_id] = total_records

        overall_total = sum(resource_counts.values())
        request.update_download(
            state=state_zipping, total=overall_total, resource_totals=resource_counts
        )

        # create a list of files that should be added to the zip, this should include the manifest
        files_to_zip = ['manifest.json'] + os.listdir(target_dir)

        # add the final data to the manifest
        manifest['total_records'] = overall_total
        manifest['files'] = files_to_zip
        end = datetime.now()
        manifest['start'] = start.isoformat()
        manifest['end'] = end.isoformat()
        manifest['duration_in_seconds'] = (end - start).total_seconds()

        # write the manifest out
        with open(os.path.join(target_dir, 'manifest.json'), 'w', encoding='utf8') as f:
            json.dump(manifest, f, sort_keys=True, indent=2, ensure_ascii=False)

        # zip up the files into the downloads directory
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, True) as z:
            for filename in files_to_zip:
                z.write(os.path.join(target_dir, filename), arcname=filename)

        # drop the user an email to let them know their download is ready
        try:
            send_email(request, zip_name)
        except (mailer.MailerException, socket.error):
            raise

        request.update_download(state=state_complete)
    except Exception as error:
        error_message = str(format_exception_only(type(error), error)[-1].strip())
        request.update_download(state=state_failed, error=error_message)
        raise
    finally:
        # remove the temp dir we were using
        shutil.rmtree(target_dir)


def send_email(request, zip_name):
    """
    Sends an email to the email address in the passed request informing them that a
    download has completed and providing them with a link to go get it from.

    :param request: the DownloadRequest object
    :param zip_name: the name of the zip file that has been created
    """
    # get the templates as strings
    templates = (default_body, default_html_body)
    for plugin in PluginImplementations(IVersionedDatastoreDownloads):
        templates = plugin.download_modify_email_templates(*templates)

    site_url = toolkit.config.get('ckan.site_url')
    context = {
        'site_url': site_url,
        'download_url': f'{site_url}/downloads/{zip_name}',
    }

    # allow plugins to modify the context passed to the templates
    for plugin in PluginImplementations(IVersionedDatastoreDownloads):
        context = plugin.download_modify_email_template_context(request, context)

    # render both templates
    body, body_html = (Template(template).render(**context) for template in templates)

    # vend
    mailer.mail_recipient(
        recipient_email=request.email_address,
        recipient_name='Downloader',
        subject='Data download',
        body=body,
        body_html=body_html,
    )
