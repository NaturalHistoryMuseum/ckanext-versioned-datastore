from datetime import datetime

from ckan.plugins import toolkit
from splitgill.search import create_version_query
from splitgill.utils import to_timestamp
from elasticsearch_dsl import Search

from .meta import help, schema
from ckantools.decorators import action
from ...lib import common
from ...lib.basic_query.search import create_search
from ...lib.datastore_utils import (
    prefix_resource,
    is_datastore_resource,
    get_public_alias_name,
)
from ...lib.query.schema import get_latest_query_version


@action(
    schema.datastore_get_record_versions(), help.datastore_get_record_versions, get=True
)
def datastore_get_record_versions(id, resource_id):
    """
    Retrieves all the versions of the record with the given id in the given resource.
    The versions are returned as a list in ascending order.

    :param id: the id of the record
    :param resource_id: the id of the resource the record is in
    :return: a list of versions in ascending order
    """
    index_name = prefix_resource(resource_id)
    return common.SEARCH_HELPER.get_record_versions(index_name, int(id))


@action(schema.datastore_search(), help.datastore_get_resource_versions, get=True)
def datastore_get_resource_versions(
    resource_id, context, data_dict, original_data_dict
):
    """
    Retrieves all the versions of the given resource when under the given search. Note
    that the schema used for this action is the same as the datastore_search schema. The
    return is a dict including the version timestamp, the number of records modified in
    the version and the total records at the version.

    :param resource_id: the id of the resource to examine
    :param context: the context dict from the action call
    :param data_dict: the data_dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :return:
    """
    original_data_dict, data_dict, version, search = create_search(
        context, data_dict, original_data_dict
    )
    index_name = prefix_resource(resource_id)

    data = common.SEARCH_HELPER.get_index_version_counts(index_name, search=search)

    search = search.using(common.ES_CLIENT).index(index_name)[0:0]
    for result in data:
        version = result['version']
        count = search.filter(create_version_query(version)).count()
        result['count'] = count
    return data


@action(
    schema.datastore_get_rounded_version(), help.datastore_get_rounded_version, get=True
)
def datastore_get_rounded_version(resource_id, version=None):
    """
    Retrieve the closest version of the resource's data to the given version when
    rounding down.

    :param resource_id: the id of the resource
    :param version: the version timestamp. If None (the default) the latest version of the resource
                    is returned
    :return: the rounded version timestamp
    """
    index_name = prefix_resource(resource_id)
    return common.SEARCH_HELPER.get_rounded_versions([index_name], version)[index_name]


@action(
    schema.datastore_is_datastore_resource(),
    help.datastore_is_datastore_resource,
    get=True,
)
def datastore_is_datastore_resource(resource_id):
    """
    Checks whether the given resource id is in the datastore or not.

    :param resource_id: the resource to check
    :return: True if it is, False if not
    """
    return is_datastore_resource(resource_id)


@action({}, help.datastore_get_latest_query_schema_version, get=True)
def datastore_get_latest_query_schema_version():
    """
    Simply returns the latest available query schema version.

    :return: the query schema version
    """
    return get_latest_query_version()


@action(schema.datastore_count(), help.datastore_count, get=True)
def datastore_count(resource_ids=None, version=None):
    if version is None:
        version = to_timestamp(datetime.now())
    if resource_ids is None:
        resource_ids = ['*']

    indexes = [get_public_alias_name(resource_id) for resource_id in resource_ids]
    search = Search(using=common.ES_CLIENT, index=indexes).filter(
        create_version_query(version)
    )

    return search.count()
