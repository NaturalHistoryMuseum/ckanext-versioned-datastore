from ckan.plugins import toolkit
from ckantools.decorators import action

from ckanext.versioned_datastore.lib.downloads.download import DownloadRunManager
from ckanext.versioned_datastore.lib.downloads.notifiers import validate_notifier_args
from ckanext.versioned_datastore.logic.download import helptext, schema
from ckanext.versioned_datastore.logic.download.arg_objects import (
    DerivativeArgs,
    NotifierArgs,
    QueryArgs,
    ServerArgs,
)
from ckanext.versioned_datastore.model.downloads import DownloadRequest


@action(schema.vds_download_queue(), helptext.vds_download_queue)
def vds_download_queue(
    context,
    query: QueryArgs,
    file: DerivativeArgs,
    server: ServerArgs = None,
    notifier: NotifierArgs = None,
):
    """
    Queues a download and returns information about the job and status.

    :param context: the CKAN action context
    :param query: the query as a QueryArgs object
    :param file: the file options as a DerivativeArgs object
    :param server: the server options as a ServerArgs object (default is None)
    :param notifier: the notifier options as a NotifierArgs object (default is None)
    :returns: a dict of information about the download
    """
    server = server or ServerArgs(**ServerArgs.defaults)
    notifier = notifier or NotifierArgs(**NotifierArgs.defaults)

    validate_notifier_args(notifier.type, notifier.type_args)

    if server.custom_filename:
        # check if admin
        try:
            toolkit.check_access('vds_custom_download_filename', context)
        except toolkit.NotAuthorized:
            server.custom_filename = None

    download_runner = DownloadRunManager(
        query_args=query,
        derivative_args=file,
        server_args=server,
        notifier_args=notifier,
    )

    job = toolkit.enqueue_job(
        download_runner.run,
        queue='download',
        title=download_runner.request.created.strftime('%Y-%m-%d %H:%M:%S'),
        rq_kwargs={'timeout': '24h'},
    )

    return {
        'queued_at': job.enqueued_at.isoformat(),
        'job_id': job.id,
        'download_id': download_runner.request.id,
        'status_json': toolkit.url_for(
            'datastore_status.download_status_json',
            download_id=download_runner.request.id,
            qualified=True,
        ),
        'status_html': toolkit.url_for(
            'datastore_status.download_status',
            download_id=download_runner.request.id,
            qualified=True,
        ),
    }


@action(schema.vds_download_regenerate(), helptext.vds_download_regenerate)
def vds_download_regenerate(
    context,
    download_id,
    server: ServerArgs = None,
    notifier: NotifierArgs = None,
):
    """
    Regenerates a download.

    :param context: the CKAN action context
    :param download_id: the download ID
    :param server: the server options as a ServerArgs object (default is None)
    :param notifier: the notifier options as a NotifierArgs object (default is None)
    :returns: a dict of information about the download
    """
    # find the download request first
    original_request = DownloadRequest.get(download_id)
    if original_request is None:
        raise toolkit.ObjectNotFound(f'Download "{download_id}" cannot be found.')

    server_args = ServerArgs.defaults.copy()
    if original_request.server_args is not None:
        server_args.update(**original_request.server_args)
    server = server or ServerArgs(**server_args)

    notifier = notifier or NotifierArgs(**NotifierArgs.defaults)

    query = QueryArgs(
        query=original_request.core_record.query,
        query_version=original_request.core_record.query_version,
        resource_ids=original_request.core_record.get_resource_ids(),
        version=original_request.core_record.get_version(),
    )

    file = DerivativeArgs(
        format=original_request.derivative_record.format,
        **original_request.derivative_record.options,
    )

    return vds_download_queue(context, query, file, server, notifier)
