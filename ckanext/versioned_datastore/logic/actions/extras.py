from datetime import datetime

from ckan.plugins import toolkit
from splitgill.search import create_version_query
from splitgill.utils import to_timestamp, chunk_iterator
from elasticsearch_dsl import Search, MultiSearch

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
    schema.datastore_get_record_versions(),
    help.datastore_get_record_versions,
    toolkit.side_effect_free,
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


@action(
    schema.datastore_search(),
    help.datastore_get_resource_versions,
    toolkit.side_effect_free,
)
def datastore_get_resource_versions(
    resource_id, context, data_dict, original_data_dict
):
    """
    Retrieves all the versions of the given resource when under the given search. Note
    that the schema used for this action is the same as the datastore_search schema. The
    return is a list of dicts each of which includes the version timestamp, the number
    of records modified in the version and the total records at the version.

    :param resource_id: the id of the resource to examine
    :param context: the context dict from the action call
    :param data_dict: the data_dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :return: a list of dicts
    """
    original_data_dict, data_dict, version, search = create_search(
        context, data_dict, original_data_dict
    )
    index_name = prefix_resource(resource_id)

    # this gives us every version in the index, plus the number of changes in that
    # version (changes include new records and changed records)
    counts = common.SEARCH_HELPER.get_index_version_counts(index_name, search=search)

    # each of the dicts in the counts list above contains the version and the number of
    # changes, but not the number of records in that version, we want to add this to
    # the dicts. To do this we will run msearches against elasticsearch so that we can
    # batch up the searches and get better performance (there could be 1000s of
    # versions to count after all). This variable simply defines how many searches to do
    # in each msearch batch
    multisearch_chunk_size = 100

    for details_chunk in chunk_iterator(counts, multisearch_chunk_size):
        multisearch = MultiSearch(using=common.ES_CLIENT, index=index_name)
        for details in details_chunk:
            multisearch = multisearch.add(
                Search()[0:0].filter(create_version_query(details["version"]))
            )
        results = multisearch.execute()
        # update the count details we got from splitgill with the actual record count
        for detail, result in zip(details_chunk, results):
            detail["count"] = result.hits.total

    return counts


@action(
    schema.datastore_get_rounded_version(),
    help.datastore_get_rounded_version,
    toolkit.side_effect_free,
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
    toolkit.side_effect_free,
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
