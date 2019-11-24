from ckan.plugins import toolkit
from eevee.search import create_version_query

from . import help
from .utils import action
from .. import schema
from ...lib import common
from ...lib.basic_query.search import create_search
from ...lib.datastore_utils import prefix_resource


@action(schema.datastore_get_record_versions(), help.datastore_get_record_versions,
        toolkit.side_effect_free)
def datastore_get_record_versions(id, resource_id):
    '''
    Retrieves all the versions of the record with the given id in the given resource. The versions
    are returned as a list in ascending order.

    :param id: the id of the record
    :param resource_id: the id of the resource the record is in
    :return: a list of versions in ascending order
    '''
    index_name = prefix_resource(resource_id)
    return common.SEARCH_HELPER.get_record_versions(index_name, int(id))


@action(schema.datastore_search(), help.datastore_get_resource_versions, toolkit.side_effect_free)
def datastore_get_resource_versions(resource_id, context, data_dict, original_data_dict):
    '''
    Retrieves all the versions of the given resource when under the given search. Note that the
    schema used for this action is the same as the datastore_search schema. The return is a dict
    including the version timestamp, the number of records modified in the version and the total
    records at the version.

    :param resource_id: the id of the resource to examine
    :param context: the context dict from the action call
    :param data_dict: the data_dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :return:
    '''
    original_data_dict, data_dict, version, search = create_search(context, data_dict,
                                                                   original_data_dict)
    index_name = prefix_resource(resource_id)

    data = common.SEARCH_HELPER.get_index_version_counts(index_name, search=search)

    search = search.using(common.ES_CLIENT).index(index_name)[0:0]
    for result in data:
        version = result[u'version']
        count = search.filter(create_version_query(version)).count()
        result[u'count'] = count
    return data


@action(schema.datastore_get_rounded_version(), help.datastore_get_rounded_version,
        toolkit.side_effect_free)
def datastore_get_rounded_version(resource_id, version=None):
    '''
    Retrieve the closest version of the resource's data to the given version when rounding down.

    :param resource_id: the id of the resource
    :param version: the version timestamp. If None (the default) the latest version of the resource
                    is returned
    :return: the rounded version timestamp
    '''
    index_name = prefix_resource(resource_id)
    return common.SEARCH_HELPER.get_rounded_versions([index_name], version)[index_name]
