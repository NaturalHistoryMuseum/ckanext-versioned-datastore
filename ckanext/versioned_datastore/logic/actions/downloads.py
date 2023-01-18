from ckantools.decorators import action

from ckan.plugins import toolkit
from .meta import help, schema
from .meta.arg_objects import ServerArgs, NotifierArgs, QueryArgs, DerivativeArgs
from ...lib.downloads.download import DownloadRunManager


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
    }
