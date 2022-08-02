from ckantools.decorators import action

from .meta import help, schema


@action(schema.datastore_queue_download(), help.datastore_queue_download)
def datastore_queue_download(context, query, file, server=None, notifier=None):
    raise NotImplemented
