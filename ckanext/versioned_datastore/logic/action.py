# -*- coding: utf-8 -*-
import copy
import logging

from ckan import logic, plugins
from ckan.lib.navl.dictization_functions import validate
from ckanext.versioned_datastore.interfaces import IVersionedDatastore
from ckanext.versioned_datastore.lib.search import create_search
from ckanext.versioned_datastore.lib.utils import searcher, get_fields, format_facets
from ckanext.versioned_datastore.logic.schema import versioned_datastore_search_schema

log = logging.getLogger(__name__)


def _validate(context, data_dict, default_schema):
    '''
    Validate the data_dict against a schema. If a schema is not available in the context (under the
    key 'schema') then the default schema is used.

    If the data_dict fails the validation process a ValidationError is raised, otherwise the
    potentially updated data_dict is returned.

    :param context: the ckan context dict
    :param data_dict: the dict to validate
    :param default_schema: the default schema to use if the context doesn't have one
    '''
    schema = context.get(u'schema', default_schema)
    data_dict, errors = validate(data_dict, schema, context)
    if errors:
        raise plugins.toolkit.ValidationError(errors)
    return data_dict


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
    # make a copy of the data dict so that we can pass it to the various plugin interface
    # implementor functions
    original_data_dict = copy.deepcopy(data_dict)

    # allow other extensions implementing our interface to modify the data_dict
    for plugin in plugins.PluginImplementations(IVersionedDatastore):
        data_dict = plugin.datastore_modify_data_dict(context, data_dict)

    # validate the data dict against our schema
    data_dict = _validate(context, data_dict, versioned_datastore_search_schema())
    # extract the resource we're going to search against. Note that this is passed to eevee as a
    # list as eevee is ready
    # for cross-resource search but this code isn't (yet)
    resources = [data_dict[u'resource_id']]
    # see if there's a version and if there is, convert it to an int
    version = None if u'version' not in data_dict else int(data_dict[u'version'])
    # create an elasticsearch-dsl search object by passing the expanded data dict
    search = create_search(**data_dict)

    # allow other extensions implementing our interface to modify the search object
    for plugin in plugins.PluginImplementations(IVersionedDatastore):
        search = plugin.datastore_modify_search(context, original_data_dict, data_dict, search)

    # run the search through eevee
    result = searcher.search(indexes=resources, search=search, version=version)

    # allow other extensions implementing our interface to modify the result object
    for plugin in plugins.PluginImplementations(IVersionedDatastore):
        result = plugin.datastore_modify_result(context, original_data_dict, data_dict, result)

    # return a dictionary containing the results and other details
    return {
        u'total': result.total,
        u'records': [hit.data for hit in result.results()],
        u'facets': format_facets(result.aggregations),
        u'fields': get_fields(data_dict['resource_id']),
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
