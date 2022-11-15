from ckan.plugins import toolkit

from .importing import (
    import_resource_data,
    ResourceImportRequest,
    ResourceDeletionRequest,
    delete_resource_data,
)
from .indexing import index_resource, ResourceIndexRequest


def queue(task, request):
    """
    Generic queueing function which ensures we set common attributes when queueing the
    task.

    :param task: the function to queue
    :param request: the queue request object
    :return: the queued job
    """
    # pass a timeout of 1 hour (3600 seconds)
    return toolkit.enqueue_job(
        task,
        args=[request],
        queue='importing',
        title=str(request),
        rq_kwargs={'timeout': 3600},
    )


def queue_import(resource, version, replace, records=None, api_key=None):
    """
    Queues a job which when run will import the data for the resource.

    :param resource: the resource we're going to import (this must be the resource dict)
    :param version: the version of the resource to import
    :param replace: whether to replace the existing data or not
    :param records: a list of dicts to import, or None if the data is coming from URL or file
    :param api_key: the api key of the user who initiated the import, this is required if the
                    package the resource is in is private and the data in the resource was uploaded
    :return: the queued job
    """
    resource_import_request = ResourceImportRequest(
        resource, version, replace, records, api_key
    )
    return queue(import_resource_data, resource_import_request)


def queue_index(resource, lower_version, upper_version):
    """
    Queues a job which when run will index the data between the given versions for the
    resource.

    :param resource: the resource we're going to index (this must be the resource dict)
    :param lower_version: the lower version to index (exclusive)
    :param upper_version: the upper version to index (inclusive)
    :return: the queued job
    """
    resource_index_request = ResourceIndexRequest(
        resource, lower_version, upper_version
    )
    return queue(index_resource, resource_index_request)


def queue_deletion(resource, version):
    """
    Queues a job which when run will delete all the data in a resource by saving a new
    version into mongo for each record where the data field is empty ({}). After this is
    done, an index is completed.

    :param resource: the resource we're going to delete (this must be the resource dict)
    :param version: the version to delete the data in
    :return: the queued job
    """
    deletion_request = ResourceDeletionRequest(resource, version)
    return queue(delete_resource_data, deletion_request)
