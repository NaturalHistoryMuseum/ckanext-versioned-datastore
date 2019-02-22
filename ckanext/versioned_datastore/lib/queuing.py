import rq

from ckan import plugins
from ckanext.rq import jobs
from ckanext.versioned_datastore.lib.importing import import_resource_data, ResourceImportRequest
from ckanext.versioned_datastore.lib.indexing.indexing import index_resource, ResourceIndexRequest

try:
    enqueue_job = plugins.toolkit.enqueue_job
except AttributeError:
    from ckanext.rq.jobs import enqueue as enqueue_job


def ensure_importing_queue_exists():
    '''
    This is a hack to get around the lack of rq Queue kwarg exposure from ckanext-rq. The default
    timeout for queues is 180 seconds in rq which is not long enough for our import tasks but the
    timeout parameter hasn't been exposed. This code creates a new queue in the ckanext-rq cache so
    that when enqueuing new jobs it is used rather than a default one. Once this bug has been fixed
    in ckan/ckanext-rq this code will be removed.

    The queue is only added if not already in existance so this is safe to call multiple times.
    '''
    name = jobs.add_queue_name_prefix(u'importing')
    if name not in jobs._queues:
        # set the timeout to 12 hours
        queue = rq.Queue(name, default_timeout=60 * 60 * 12, connection=jobs._connect())
        # add the queue to the queue cache
        jobs._queues[name] = queue


def queue(function, args):
    '''
    Generic queueing function which ensures our special queue is setup first.

    :param function: the function to queue
    :param args: the arguments for the function, this should be a list
    :return: the queued job
    '''
    ensure_importing_queue_exists()
    return enqueue_job(function, args=args, queue=u'importing')


def queue_import(resource_id, version, replace, records=None, api_key=None):
    '''
    Queues a job which when run will import the data for the resource.

    :param resource_id: the id of the resource to import
    :param version: the version of the resource to import
    :param replace: whether to replace the existing data or not
    :param records: a list of dicts to import, or None if the data is coming from URL or file
    :param api_key: the api key of the user who initiated the import, this is required if the
                    package the resource is in is private and the data in the resource was uploaded
    :return: the queued job
    '''
    resource_import_request = ResourceImportRequest(resource_id, version, replace, records, api_key)
    return queue(import_resource_data, [resource_import_request])


def queue_index(resource, lower_version, upper_version):
    '''
    Queues a job which when run will index the data between the given versions for the resource.

    :param resource: the dict for the resource we're going to index
    :param lower_version: the lower version to index (exclusive)
    :param upper_version: the upper version to index (inclusive)
    :return: the queued job
    '''
    resource_index_request = ResourceIndexRequest(resource, lower_version, upper_version)
    return queue(index_resource, [resource_index_request])
