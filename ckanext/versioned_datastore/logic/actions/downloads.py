from ckantools.decorators import action

from ckan.plugins import toolkit
from .meta import help, schema
from .meta.arg_objects import ServerArgs, NotifierArgs, QueryArgs, DerivativeArgs
from ...lib.downloads.download import DownloadRunManager
from ...model.downloads import DownloadRequest
from ...lib.downloads.notifiers import validate_notifier_args


@action(schema.datastore_queue_download(), help.datastore_queue_download)
def datastore_queue_download(
    context,
    query: QueryArgs,
    file: DerivativeArgs,
    server: ServerArgs = None,
    notifier: NotifierArgs = None,
):
    server = server or ServerArgs(**ServerArgs.defaults)
    notifier = notifier or NotifierArgs(**NotifierArgs.defaults)

    validate_notifier_args(notifier.type, notifier.type_args)

    if server.custom_filename:
        # check if admin
        try:
            toolkit.check_access('datastore_custom_download_filename', context)
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


@action(schema.datastore_regenerate_download(), help.datastore_regenerate_download)
def datastore_regenerate_download(
    context,
    download_id,
    server: ServerArgs = None,
    notifier: NotifierArgs = None,
):
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
        resource_ids_and_versions=original_request.core_record.resource_ids_and_versions,
    )

    file = DerivativeArgs(
        format=original_request.derivative_record.format,
        **original_request.derivative_record.options,
    )

    return datastore_queue_download(context, query, file, server, notifier)
