from ckan.plugins import toolkit
from datetime import datetime
from splitgill.utils import to_timestamp

from .meta import help, schema
from ckantools.decorators import action
from ...lib import common
from ...lib.datastore_utils import (
    is_resource_read_only,
    is_ingestible,
    update_privacy,
    ReadOnlyResourceException,
    InvalidVersionException,
    is_datastore_resource,
)
from ...lib.importing import stats
from ...lib.importing.indexing import DatastoreIndex
from ...lib.importing.queuing import queue_index, queue_import, queue_deletion
from ...lib.importing.utils import check_version_is_valid


@action(schema.datastore_create(), help.datastore_create)
def datastore_create(resource_id, context):
    """
    Creates an index in elasticsearch for the given resource unless it's a read only
    index or it looks uningestible.

    :param resource_id: the resource's id
    :param context: the context dict from the action call
    :return: True if the index was created or already existed, False if not
    """
    if is_resource_read_only(resource_id):
        return False

    # lookup the resource dict
    resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
    # only create the index if the resource is ingestable
    if is_ingestible(resource):
        # note that the version parameter doesn't matter when creating the index so we can safely
        # pass None
        common.SEARCH_HELPER.ensure_index_exists(
            DatastoreIndex(common.CONFIG, resource_id, None)
        )
        # make sure the privacy is correctly setup
        update_privacy(resource_id)
        return True
    return False


@action(schema.datastore_upsert(), help.datastore_upsert)
def datastore_upsert(resource_id, replace, context, original_data_dict, version=None):
    """
    Main data ingestion function for the datastore. The URL on the given resource will
    be used to retrieve and then ingest data or, if provided, records will be ingested
    directly from the request. Data is ingested using the an rq background job and
    therefore this is an async action.

    :param resource_id: the resource to ingest the data into
    :param replace: whether to replace the data already in the resource or append to it
    :param context: the context dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :param version: the version of the new data, can be None (default) but if not must be newer
                    than the latest version of the resource
    :return: information about the background job that is handling the ingestion
    """
    # this comes through as junk if it's not removed before validating. This happens because the
    # data dict is flattened during validation, but why this happens is unclear.
    records = original_data_dict.get('records', None)

    if is_resource_read_only(resource_id):
        raise ReadOnlyResourceException('This resource has been marked as read only')

    if version is None:
        version = to_timestamp(datetime.now())

    # check that the version is valid
    if not check_version_is_valid(resource_id, version):
        raise InvalidVersionException(
            'The new version must be newer than current version'
        )

    # get the current user
    user = toolkit.get_action('user_show')(context, {'id': context['user']})

    # queue the resource import job
    resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
    job = queue_import(resource, version, replace, records, user['apikey'])

    return {
        'queued_at': job.enqueued_at.isoformat(),
        'job_id': job.id,
    }


@action(schema.datastore_delete(), help.datastore_delete)
def datastore_delete(resource_id, context, version=None):
    """
    Deletes the resource from the datastore. In reality the resource data is maintained
    in its index but the latest version of all records is set to an empty record. This
    means that the old data is still accessible to ensure searches using versions before
    the deletion still work but searches at the latest version or later will return no
    records. The deletion work is done by an rq background job and therefore this is an
    async action.

    :param resource_id: the id of the resource to delete
    :param context: the context dict from the action call
    :param version: the to mark the deletion at
    :return: a dict containing info about the background job that is doing the delete
    """
    # if the requested deletion version is missing, default to now
    if version is None:
        version = to_timestamp(datetime.now())

    if is_resource_read_only(resource_id):
        raise toolkit.ValidationError('This resource has been marked as read only')

    # queue the job
    resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
    job = queue_deletion(resource, version)
    return {
        'queued_at': job.enqueued_at.isoformat(),
        'job_id': job.id,
    }


@action(schema.datastore_reindex(), help.datastore_reindex)
def datastore_reindex(resource_id, context):
    """
    Reindexes that data in the given resource, this involves recreating all versions of
    each record in elasticsearch using the data in mongo. The reindexing work is done by
    an rq background job and therefore this is an async action.

    :param resource_id: the resource id to reindex
    :param context: the context dict from the action call
    :return: a dict containing info about the background job that is doing the reindexing
    """
    if is_resource_read_only(resource_id):
        raise toolkit.ValidationError('This resource has been marked as read only')

    last_ingested_version = stats.get_last_ingest(resource_id)
    if last_ingested_version is None:
        raise toolkit.ValidationError('There is no ingested data for this version')

    resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
    job = queue_index(resource, None, last_ingested_version.version)

    return {
        'queued_at': job.enqueued_at.isoformat(),
        'job_id': job.id,
    }


@action(schema.datastore_ensure_privacy(), help.datastore_ensure_privacy)
def datastore_ensure_privacy(context, resource_id=None):
    """
    Ensures that the privacy settings for the given resource, or if not resource is
    provided all resources, are correct. This means ensuring that the public alias for
    the resource's index exists or doesn't depending on the owning package's privacy
    setting.

    :param context: the context dict from the action call
    :param resource_id: the id of the resource update. Can be None (the default) which means all
                        resources are updated
    :return: a dict containing the total number of resources checked and the total modified
    """
    modified = 0
    total = 0
    if resource_id is not None:
        total += 1
        if is_datastore_resource(resource_id):
            if update_privacy(resource_id):
                modified += 1
    else:
        package_data_dict = {'limit': 50, 'offset': 0}
        while True:
            # iteratively retrieve all packages and ensure their resources
            packages = toolkit.get_action('current_package_list_with_resources')(
                context, package_data_dict
            )
            if not packages:
                # we've ensured all the packages that are available
                break
            else:
                # setup the next loop so that we get the next page of results
                package_data_dict['offset'] += len(packages)
                for package in packages:
                    for resource in package.get('resources', []):
                        if resource['datastore_active']:
                            total += 1
                            if update_privacy(resource['id'], package['private']):
                                modified += 1

    return {'modified': modified, 'total': total}
