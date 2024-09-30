from typing import Optional, List

from ckantools.decorators import action
from elasticsearch_dsl import Q
from splitgill.indexing.fields import DocumentField

from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.importing.options import update_options
from ckanext.versioned_datastore.lib.importing.tasks import (
    queue_ingest,
    queue_delete,
    queue_sync,
)
from ckanext.versioned_datastore.lib.query.search.query import DirectQuery
from ckanext.versioned_datastore.lib.query.search.request import SearchRequest
from ckanext.versioned_datastore.lib.utils import (
    is_resource_read_only,
    ReadOnlyResourceException,
)
from ckanext.versioned_datastore.logic.data import helptext, schema


@action(schema.vds_data_add(), helptext.vds_data_add)
def vds_data_add(
    context: dict, resource_id: str, replace: bool, records: Optional[List[dict]] = None
):
    """
    Add data to the datastore for the given resource. The data added will be the data
    found at the resource's URL unless a list of dict records is provided in which case
    this will be added. The addition is asynchronous and so the result returned by
    calling this action is information about the queued jobs. Two jobs will be queued,
    an ingest job to store the data found at the URL/in the provided records, and then a
    sync job, which updates the data search index making the data available for
    searching (this will only run if the ingest succeeded).

    If the replace flag is passed as True, all the existing data in the datastore for
    this resource will be deleted before the new data is added, resulting in the new
    data replacing the old data. This is the kind of behaviour users expect when
    uploading a fresh new file.

    Before the ingest and sync jobs are queued, the datastore's parsing options are
    updated if required. If they are altered before ingest, the new version will be
    returned in the result dict.

    :param context: the CKAN context
    :param resource_id: the resource ID
    :param replace: whether to replace all existing records with the new ones
    :param records: an optional list of dicts representing record data to be added
                    instead of the data found at the resource's URL
    :return: a dict containing information about the add
    """
    if is_resource_read_only(resource_id):
        raise ReadOnlyResourceException("This resource has been marked as read only")

    # make sure the options are in sync
    new_options_version = update_options(resource_id)

    user = toolkit.get_action("user_show")(context, {"id": context["user"]})
    resource = toolkit.get_action("resource_show")(context, {"id": resource_id})
    ingest_job, sync_job = queue_ingest(resource, replace, user["apikey"], records)

    return {
        "new_options_version": new_options_version,
        "queued_at": ingest_job.enqueued_at.isoformat(),
        "ingest_job_id": ingest_job.id,
        "sync_job_id": sync_job.id,
    }


@action(schema.vds_data_delete(), helptext.vds_data_delete)
def vds_data_delete(context: dict, resource_id: str):
    if is_resource_read_only(resource_id):
        raise ReadOnlyResourceException("This resource has been marked as read only")

    resource = toolkit.get_action("resource_show")(context, {"id": resource_id})
    delete_job, sync_job = queue_delete(resource)

    return {
        "queued_at": delete_job.enqueued_at.isoformat(),
        "delete_job_id": delete_job.id,
        "sync_job_id": sync_job.id,
    }


@action(schema.vds_data_sync(), helptext.vds_data_sync)
def vds_data_sync(context: dict, resource_id: str, full: bool = False):
    if is_resource_read_only(resource_id):
        raise ReadOnlyResourceException("This resource has been marked as read only")

    resource = toolkit.get_action("resource_show")(context, {"id": resource_id})
    job = queue_sync(resource, full)

    return {
        "queued_at": job.enqueued_at.isoformat(),
        "job_id": job.id,
    }


# for compat reasons
@action(schema.vds_data_get(), helptext.vds_data_get, get=True)
def record_show(resource_id: str, record_id: str, version: Optional[int] = None):
    return vds_data_get(resource_id, record_id, version)


@action(schema.vds_data_get(), helptext.vds_data_get, get=True)
def vds_data_get(resource_id: str, record_id: str, version: Optional[int] = None):
    query = DirectQuery(
        [resource_id], version, Q("term", **{DocumentField.ID: record_id})
    )
    request = SearchRequest(query, size=1)
    response = request.run()
    try:
        return {
            "data": response.data[0],
            # TODO: do we actually need to add the fields?
            # get_fields(resource_id, request.query.version)
            "resource_id": resource_id,
        }
    except IndexError:
        # if we don't have a result, raise not found
        raise toolkit.ObjectNotFound