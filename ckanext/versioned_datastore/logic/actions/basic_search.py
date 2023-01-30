from datetime import datetime

from ckan.plugins import toolkit, PluginImplementations
from splitgill.indexing.utils import DOC_TYPE
from splitgill.search import create_version_query
from splitgill.utils import to_timestamp
from elasticsearch import RequestError
from elasticsearch_dsl import A, Search

from .meta import help, schema
from ckantools.decorators import action
from ...interfaces import IVersionedDatastore
from ...lib.basic_query.search import create_search
from ...lib.basic_query.utils import run_search, get_fields, format_facets
from ...lib.datastore_utils import prefix_field, prefix_resource, get_last_after
from ...lib.query.query_log import log_query


# these are the keys we're interested in logging from the data dict
_query_log_keys = ('q', 'filters')


@action(schema.datastore_search(), help.datastore_search, get=True)
def datastore_search(context, data_dict, original_data_dict):
    """
    Searches the datastore using a query schema similar to the standard CKAN datastore
    query schema, but with versioning.

    :param context: the context dict from the action call
    :param data_dict: the data_dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :return: a dict including the search results amongst other things
    """
    original_data_dict, data_dict, version, search = create_search(
        context, data_dict, original_data_dict
    )
    resource_id = data_dict['resource_id']
    index_name = prefix_resource(resource_id)

    # if the version is None, default it to the current timestamp
    if version is None:
        version = to_timestamp(datetime.now())

    # add the version filter to the query
    search = search.filter(create_version_query(version))

    # if the run query option is false (default to true if not present) then just return the query
    # we would have run against elasticsearch instead of actually running it. This is useful for
    # running the query outside of ckan, for example on a tile server.
    if not data_dict.get('run_query', True):
        return {
            'indexes': [index_name],
            'search': search.to_dict(),
        }
    else:
        result = run_search(search, [index_name])

        # allow other extensions implementing our interface to modify the result
        for plugin in PluginImplementations(IVersionedDatastore):
            result = plugin.datastore_modify_result(
                context, original_data_dict, data_dict, result
            )

        # add the actual result object to the context in case the caller is an extension and they
        # have used one of the interface hooks to alter the search object and include, for example,
        # an aggregation
        context['versioned_datastore_query_result'] = result

        # get the fields
        mapping, fields = get_fields(resource_id, version)
        # allow other extensions implementing our interface to modify the field definitions
        for plugin in PluginImplementations(IVersionedDatastore):
            fields = plugin.datastore_modify_fields(resource_id, mapping, fields)

        query_for_logging = {}
        for key in _query_log_keys:
            if data_dict.get(key, None):
                query_for_logging[key] = data_dict[key]
        log_query(query_for_logging, 'basicsearch')

        # return a dictionary containing the results and other details
        return {
            'total': result.hits.total,
            'records': [hit.data.to_dict() for hit in result],
            'facets': format_facets(result.aggs.to_dict()),
            'fields': fields,
            'raw_fields': mapping['mappings'][DOC_TYPE]['properties']['data'][
                'properties'
            ],
            'after': get_last_after(result.hits),
            '_backend': 'versioned-datastore',
        }


@action(
    schema.datastore_autocomplete(),
    help.datastore_autocomplete,
    toolkit.side_effect_free,
)
def datastore_autocomplete(context, data_dict, original_data_dict):
    """
    Runs a search to find autocomplete values based on the provided prefix.

    :param context: the context dict from the action call
    :param data_dict: the data_dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :return: a dict containing a list of results and an after value for the next page of results
    """
    # extract the fields specifically needed for setting up the autocomplete query
    field = data_dict.pop('field')
    term = data_dict.pop('term')
    after = data_dict.pop('after', None)
    # default to a size of 20 results
    size = data_dict.pop('limit', 20)
    # ensure the search doesn't respond with any hits cause we don't need them
    data_dict['limit'] = 0
    # remove the offset if one was passed as we don't need it
    data_dict.pop('offset', None)

    # now build the search object against the normal search code
    original_data_dict, data_dict, version, search = create_search(
        context, data_dict, original_data_dict
    )
    # get the index we're going to search against
    index_name = prefix_resource(data_dict['resource_id'])

    # add the autocompletion query part which takes the form of a prefix search
    search = search.filter('prefix', **{prefix_field(field): term})
    # modify the search so that it has the aggregation required to get the autocompletion results
    search.aggs.bucket(
        'field_values',
        'composite',
        size=size,
        sources={field: A('terms', field=prefix_field(field), order='asc')},
    )
    # if there's an after included, add it into the aggregation
    if after:
        search.aggs['field_values'].after = {field: after}

    # run the search (this adds the version to the query too)
    result = run_search(search, [index_name], version)

    # get the results we're interested in
    agg_result = result.aggs.to_dict()['field_values']
    # return a dict of results, but only include the after details if there are any to include
    return_dict = {
        'values': [bucket['key'][field] for bucket in agg_result['buckets']],
    }
    if 'after_key' in agg_result:
        return_dict['after'] = agg_result['after_key'][field]
    return return_dict


@action(
    schema.datastore_search(), help.datastore_query_extent, toolkit.side_effect_free
)
def datastore_query_extent(context, data_dict, original_data_dict):
    """
    Given the parameters for a datastore_query, finds the geographic extent of the
    query's results.

    :param context: the context dict from the action call
    :param data_dict: the data_dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :return: a dict containing the total number of matches for the query, the total number of
             matches with geo data and the bounds of the query
    """
    # ensure the search doesn't respond with any hits cause we don't need them and override two
    # unused params
    data_dict['limit'] = 0
    data_dict.pop('offset', None)
    data_dict.pop('after', None)

    # now build the search object against the normal search code
    original_data_dict, data_dict, version, search = create_search(
        context, data_dict, original_data_dict
    )
    # if we don't have a version, set to now
    if version is None:
        version = to_timestamp(datetime.now())

    # get the index we're going to search against
    index_name = prefix_resource(data_dict['resource_id'])

    # add our bounds and geo count aggregations
    search.aggs.bucket('bounds', 'geo_bounds', field='meta.geo', wrap_longitude=False)
    search.aggs.bucket('geo_count', 'value_count', field='meta.geo')

    # add version filter and run the search
    result = run_search(search, [index_name], version)
    agg_result = result.aggs.to_dict()

    # create a dict of results for return
    to_return = {
        'total_count': result.hits.total,
        'geom_count': agg_result['geo_count']['value'],
    }

    # extract and add the bounds info from the aggregations if there is any
    if agg_result['geo_count']['value'] > 0:
        top_left = agg_result['bounds']['bounds']['top_left']
        bottom_right = agg_result['bounds']['bounds']['bottom_right']
        to_return['bounds'] = [[p['lat'], p['lon']] for p in (top_left, bottom_right)]

    return to_return


@action(
    schema.datastore_search_raw(), help.datastore_search_raw, toolkit.side_effect_free
)
def datastore_search_raw(
    resource_id,
    context,
    data_dict,
    original_data_dict,
    search=None,
    version=None,
    raw_result=False,
    include_version=True,
):
    """
    Searches the datastore using a raw elasticsearch query.

    :param resource_id: the id of the resource to search
    :param context: the context dict from the action call
    :param data_dict: the data_dict from the action call
    :param original_data_dict: the data_dict before it was validated
    :param search: the elasticsearch query to run
    :param version: the version of the data to query against
    :param raw_result: whether to return the result as a raw elasticsearch result, or format it in
                       the same way as a normal datastore_search call would
    :param include_version: whether to include the version in the search or not
    :return: a dict containing the results of the search
    """
    if search is None:
        search = {}
    if version is None:
        version = to_timestamp(datetime.now())
    index_name = prefix_resource(resource_id)
    search = Search.from_dict(search)

    try:
        # the user has asked for a raw result and that the version filter is not included
        if raw_result and not include_version:
            version = None

        # run the query passing the version which will either be the requested version, the current
        # timestamp or None if no version filter should be included in the search
        result = run_search(search, index_name, version)

        if raw_result:
            return result.to_dict()

        # allow other extensions implementing our interface to modify the result object
        for plugin in PluginImplementations(IVersionedDatastore):
            result = plugin.datastore_modify_result(
                context, original_data_dict, data_dict, result
            )

        # add the actual result object to the context in case the caller is an extension and
        # they have used one of the interface hooks to alter the search object and include, for
        # example, an aggregation
        context['versioned_datastore_query_result'] = result

        # get the fields
        mapping, fields = get_fields(resource_id, version)
        # allow other extensions implementing our interface to modify the field definitions
        for plugin in PluginImplementations(IVersionedDatastore):
            fields = plugin.datastore_modify_fields(resource_id, mapping, fields)

        # return a dictionary containing the results and other details
        return {
            'total': result.hits.total,
            'records': [hit.data.to_dict() for hit in result],
            'facets': format_facets(result.aggs.to_dict()),
            'fields': fields,
            'raw_fields': mapping['mappings'][DOC_TYPE]['properties']['data'][
                'properties'
            ],
            'after': get_last_after(result.hits),
            '_backend': 'versioned-datastore',
        }
    except RequestError as e:
        raise toolkit.ValidationError(str(e))
