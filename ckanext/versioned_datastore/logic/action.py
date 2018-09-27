# -*- coding: utf-8 -*-
import base64
import logging

from elasticsearch_dsl import A

from ckan import logic, plugins
from ckanext.versioned_datastore.interfaces import IVersionedDatastore
from ckanext.versioned_datastore.lib.search import create_search, prefix_field
from ckanext.versioned_datastore.lib.utils import get_searcher, get_fields, format_facets, validate
from ckanext.versioned_datastore.logic import schema

log = logging.getLogger(__name__)


@logic.side_effect_free
def datastore_search(context, data_dict):
    '''
    This action allows you to search data in a resource. It is designed to function in a similar way
    to CKAN's core datastore_search but with a few extra bells and whistles, most prominently
    versioning. This allows the resource to be searched at any moment in it's lifespan and have the
    data as it looked at that moment returned, even if it has changed since.

    If the resource to be searched is private then appropriate authorization is required.

    Note that in the parameters listed below spaces should only be included if part of a field name,
    so, for example, don't include any spaces in comma separated lists unless needed.

    :param resource_id: id of the resource to be searched against
    :type resource_id: string
    :param q: full text query. If a string is passed, all fields are searched with the value. If a
          dict is passed each of the fields and values contained within will be searched as
          required (e.g. {"field1": "a", "field2": "b"}).
    :type q: string or dictionary
    :param filters: a dictionary of conditions that must be met to match a record
                    (e.g {"field1": "a", "field2": "b"}) (optional)
    :type filters: dictionary
    :param limit: maximum number of records to return (optional, default: 100)
    :type limit: int
    :param offset: offset this number of records (optional)
    :type offset: int
    :param fields: fields to return for each record (optional, default: all fields are returned)
    :type fields: list or comma separated string
    :param sort: list of field names with ordering. Ordering is ascending by default, if descending
                 is required, add "desc" after the field name
                 e.g.: "fieldname1,fieldname2 desc" sorts by fieldname1 asc and fieldname2 desc
    :type sort: list or comma separated string
    :param version: version to search at, if not provided the current version of the data is
                   searched.
    :type version: int, number of milliseconds (not seconds!) since UNIX epoch
    :param facets: if present, the top 10 most frequent values for each of the fields in this list
                   will be returned along with estimated counts for each value. Calculating these
                   results has a reasonable overhead so only include this parameter if you need it
    :type facets: list or comma separated string
    :param facet_limits: if present, specifies the number of top values to retrieve for the facets
                        listed within. The default number will be used if this parameter is not
                        specified or if a facet in the facets list does not appear in this dict. For
                        example, with this facet list ['facet1', 'facet2', 'facet3', 'facet4'], and
                        this facet_limits dict: {'facet1': 50, 'facet4': 10}, facet1 and facet4
                        would be limited to top 50 and 10 values respectively, whereas facet2 and
                        facet3 would be limited to the default of the top 10.
    :type facet_limits: a dict


    **Results:**

    The result of this action is a dictionary with the following keys:

    :rtype: A dict with the following keys
    :param fields: fields/columns and their extra metadata
    :type fields: list of dicts
    :param total: number of total matching records
    :type total: int
    :param records: list of matching results
    :type records: list of dicts
    :param facets: list of fields and their top 10 values, if requested
    :type facets: dict
    '''
    original_data_dict, data_dict, search = create_search(context, data_dict)
    resource_id = data_dict[u'resource_id']
    # see if there's a version and if there is, convert it to an int
    version = None if u'version' not in data_dict else int(data_dict[u'version'])

    # run the search through eevee. Note that we pass the indexes to eevee as a list as eevee is
    # ready for cross-resource search but this code isn't (yet)
    result = get_searcher().search(indexes=[resource_id], search=search, version=version)

    # allow other extensions implementing our interface to modify the result object
    for plugin in plugins.PluginImplementations(IVersionedDatastore):
        result = plugin.datastore_modify_result(context, original_data_dict, data_dict, result)

    # get the fields
    mapping, fields = get_fields(resource_id)
    # allow other extensions implementing our interface to modify the field definitions
    for plugin in plugins.PluginImplementations(IVersionedDatastore):
        fields = plugin.datastore_modify_fields(resource_id, mapping, fields)

    # return a dictionary containing the results and other details
    return {
        u'total': result.total,
        u'records': [hit.data for hit in result.results()],
        u'facets': format_facets(result.aggregations),
        u'fields': fields,
    }


def datastore_create(context, data_dict):
    # TODO: implement
    pass


def datastore_upsert(context, data_dict):
    # TODO: implement
    pass


def datastore_delete(context, data_dict):
    # TODO: implement
    pass


@logic.side_effect_free
def datastore_get_record_versions(context, data_dict):
    '''
    Given a record id and an resource it appears in, returns the version timestamps available for
    that record in ascending order.

    Data dict params:
    :param resource_id: resource id that the record id appears in
    :type resource_id: string
    :param id: the id of the record
    :type id: integer

    **Results:**

    :returns: a list of versions
    :rtype: list
    '''
    data_dict = validate(context, data_dict, schema.datastore_get_record_versions_schema())
    return get_searcher().get_versions(data_dict['resource_id'], int(data_dict['id']))


@logic.side_effect_free
def datastore_autocomplete(context, data_dict):
    '''
    Provides autocompletion results against a specific field in a specific resource.

    **Data dict params:**

    :param resource_id: id of the resource to be searched against
    :type resource_id: string
    :param q: full text query. If a string is passed, all fields are searched with the value. If a
          dict is passed each of the fields and values contained within will be searched as
          required (e.g. {"field1": "a", "field2": "b"}).
    :type q: string or dictionary
    :param filters: a dictionary of conditions that must be met to match a record
                    (e.g {"field1": "a", "field2": "b"}) (optional)
    :type filters: dictionary
    :param limit: maximum number of records to return (optional, default: 100)
    :type limit: int
    :param after: search after offset value as a base64 encoded string
    :type after: string
    :param field: the field to autocomplete against
    :type field: string
    :param term: the search term for the autocompletion
    :type term: string
    :param version: version to search at, if not provided the current version of the data is
                   searched.
    :type version: int, number of milliseconds (not seconds!) since UNIX epoch


    **Results:**

    :returns: a dict containing the list of values and an after value for the next page's results
    :rtype: dict
    '''
    # ensure the data dict is valid against our autocomplete action schema
    data_dict = validate(context, data_dict, schema.datastore_autocomplete_schema())

    # extract the fields specifically needed for setting up the autocomplete query
    field = data_dict.pop(u'field')
    term = data_dict.pop(u'term')
    after = data_dict.pop(u'after', None)
    # default to a size of 20 results
    size = data_dict.pop(u'limit', 20)
    # ensure the search doesn't respond with any hits cause we don't need them
    data_dict[u'limit'] = 0

    # now build the search object against the normal search code
    _original_data_dict, data_dict, search = create_search(context, data_dict)
    # get the resource id we're going to search against
    resource_id = data_dict[u'resource_id']
    # see if there's a version and if there is, convert it to an int
    version = None if u'version' not in data_dict else int(data_dict[u'version'])

    # add the autocompletion query part which takes the form of a prefix search
    search = search.filter(u'prefix', **{prefix_field(field): term})
    # modify the search so that it has the aggregation required to get the autocompletion results
    search.aggs.bucket(u'field_values', u'composite', size=size,
                       sources={field: A(u'terms', field=prefix_field(field), order=u'asc')})
    # if there's an after included, add it into the aggregation
    if after:
        search.aggs[u'field_values'].after = {field: after}

    # run the search
    result = get_searcher().search(indexes=[resource_id], search=search, version=version)
    # get the results we're interested in
    agg_result = result.aggregations[u'field_values']
    # return a dict of results, but only include the after details if there are any to include
    return_dict = {
        u'values': [bucket[u'key'][field] for bucket in agg_result[u'buckets']],
    }
    if u'after_key' in agg_result:
        return_dict[u'after'] = agg_result[u'after_key'][field]
    return return_dict
