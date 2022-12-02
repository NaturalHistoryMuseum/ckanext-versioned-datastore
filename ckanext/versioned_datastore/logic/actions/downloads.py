from ckan.plugins import toolkit
from ckantools.decorators import action

from .meta import help, schema
from .meta.arg_objects import ServerArgs, NotifierArgs
from ...lib.downloads.download import DownloadRunManager


@action(schema.datastore_queue_download(), help.datastore_queue_download)
def datastore_queue_download(context, query, file, server=None, notifier=None):
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
        rq_kwargs={'timeout': 3600},
    )

    return {
        'queued_at': job.enqueued_at.isoformat(),
        'job_id': job.id,
        'download_id': download_runner.request.id,
    }
