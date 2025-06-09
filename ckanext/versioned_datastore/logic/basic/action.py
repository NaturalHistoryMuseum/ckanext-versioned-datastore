from typing import Optional

from ckan.plugins import toolkit
from ckantools.decorators import action
from elasticsearch_dsl import A, Q
from splitgill.search import ALL_POINTS, keyword

from ckanext.versioned_datastore.logic.basic import helptext, schema
from ckanext.versioned_datastore.logic.basic.utils import (
    format_facets,
    get_fields,
    make_request,
)


# for compat reasons
@action(schema.vds_basic_query(), helptext.vds_basic_query, get=True)
def datastore_search(
    data_dict: dict,
    resource_id: str,
    run_query: Optional[bool] = True,
):
    """
    Performs a basic query on one resource with parameters and return value compatible
    with CKAN's datastore.

    Just calls vds_basic_query.
    """
    return vds_basic_query(data_dict, resource_id, run_query)


@action(schema.vds_basic_query(), helptext.vds_basic_query, get=True)
def vds_basic_query(
    data_dict: dict,
    resource_id: str,
    run_query: Optional[bool] = True,
):
    """
    Performs a basic query on one resource using a simple query language which is in
    line with CKAN's default datastore extension.

    :param data_dict: the data dict passed to this action
    :param resource_id: the ID of the resource to search
    :param run_query: whether to run the query, or just return information about what
        would have been run
    :returns: a dict
    """
    request = make_request(data_dict)
    if not run_query:
        return {'indexes': request.indexes(), 'search': request.to_search().to_dict()}

    response = request.run()
    return {
        'total': response.count,
        'records': response.data,
        'facets': format_facets(response.aggs),
        'fields': get_fields(resource_id, request.query.version),
        'after': response.next_after,
    }


@action(schema.vds_basic_count(), helptext.vds_basic_query, get=True)
def vds_basic_count(data_dict: dict):
    """
    Counts the number of records that meet the given query on the resource with the
    given ID and returns this number.

    :param data_dict: the data dict passed to this action
    :returns: an integer >= 0
    """
    request = make_request(data_dict)
    request.set_no_results()
    response = request.run()
    return response.count


@action(schema.vds_basic_autocomplete(), helptext.vds_basic_autocomplete, get=True)
def vds_basic_autocomplete(data_dict: dict, field: str, term: str):
    """
    Provides autocomplete for values on the given field in the given resource under the
    given query and version restrictions. Use after and limit to paginate through
    results.

    :param data_dict: the data dict passed to this action
    :param field: the field to autocomplete values on
    :param term: a prefix to filter field values by
    :returns: a dict containing the values and optionally an after key for pagination
    """
    # extract the limit but default to a size of 20 if it's not present
    size = data_dict.pop('limit', 20)
    if not 0 < size <= 20:
        raise toolkit.ValidationError('Size must be 0 < size <= 20')
    # grab the after as we need to use it for the agg, not the query
    after = data_dict.pop('after', None)

    request = make_request(data_dict)
    request.set_no_results()
    # add the autocomplete part of the query
    request.extra_filter &= Q('prefix', **{keyword(field): term})

    # add the aggregation which gets the field values
    agg_options = dict(field=keyword(field), order='asc')
    if after is not None:
        agg_options['after'] = {field: after}
    request.add_agg(
        'field_values',
        'composite',
        size=size,
        sources={field: A('terms', **agg_options)},
    )

    response = request.run()
    agg_result = response.aggs['field_values']
    return_dict = {
        'values': [bucket['key'][field] for bucket in agg_result['buckets']],
    }
    if 'after_key' in agg_result:
        return_dict['after'] = agg_result['after_key'][field]
    return return_dict


# for compat reasons
@action(schema.vds_basic_query(), helptext.vds_basic_extent, get=True)
def datastore_query_extent(data_dict: dict):
    """
    Performs a basic query extent analysis on one resource with parameters and return
    value compatible with CKAN's datastore.

    Just calls vds_basic_extent.
    """
    return vds_basic_extent(data_dict)


@action(schema.vds_basic_extent(), helptext.vds_basic_extent, get=True)
def vds_basic_extent(data_dict: dict):
    """
    Calculates the geographic extent of the given resource using the given query.
    Returns a dict containing the overall number of records which matched the query, the
    number of records with geo data that matched the query and then the bounds as a pair
    of coordinates (top left, bottom right).

    :param data_dict: the data dict passed to this action
    :returns: a dict
    """
    request = make_request(data_dict)
    request.set_no_results()

    # get the total number of records first before we do anything else
    total = request.run().count

    # add a filter to get only records with geo data
    request.extra_filter &= Q('exists', field=ALL_POINTS)
    # add our bounds aggregation
    request.add_agg('bounds', 'geo_bounds', field=ALL_POINTS, wrap_longitude=False)

    response = request.run()

    result = {
        'total_count': total,
        'geom_count': response.count,
    }
    # extract and add the bounds info from the aggregations if there is any
    if response.count > 0:
        top_left = response.aggs['bounds']['bounds']['top_left']
        bottom_right = response.aggs['bounds']['bounds']['bottom_right']
        result['bounds'] = [[p['lat'], p['lon']] for p in (top_left, bottom_right)]

    return result
