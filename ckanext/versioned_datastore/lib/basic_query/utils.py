import copy
import json
from elasticsearch import NotFoundError
from elasticsearch_dsl import MultiSearch, Search
from splitgill.indexing.utils import DOC_TYPE
from splitgill.search import create_version_query

from ckan.lib.search import SearchIndexError
from ckan.plugins import PluginImplementations
from .. import common
from ..datastore_utils import prefix_resource, prefix_field
from ..importing.details import get_all_details
from ...interfaces import IVersionedDatastore


def run_search(search, indexes, version=None):
    """
    Convenience function to runs a search on the given indexes using the client
    available in this module.

    If the index(es) required for the search are missing then a CKAN SearchIndexError exception is
    raised.

    :param search: the elasticsearch-dsl search object
    :param indexes: either a list of index names to search in or a single index name as a string
    :param version: version to filter the search results to, optional
    :return: the result of running the query
    """
    try:
        if version is not None:
            search = search.filter(create_version_query(version))
        if isinstance(indexes, str):
            indexes = [indexes]
        return search.index(indexes).using(common.ES_CLIENT).execute()
    except NotFoundError as e:
        raise SearchIndexError(e.error)


def format_facets(aggs):
    '''
    Formats the facet aggregation result into the format we require. Specifically we expand the
    buckets out into a dict that looks like this:

        {
            "facet1": {
                "details": {
                    "sum_other_doc_count": 34,
                    "doc_count_error_upper_bound": 3
                },
                "values": {
                    "value1": 1,
                    "value2": 4,
                    "value3": 1,
                    "value4": 2,
                }
            },
            etc
        }

    etc.

    :param aggs: the aggregation dict returned from splitgill/elasticsearch
    :return: the facet information as a dict
    '''
    facets = {}
    for facet, details in aggs.items():
        facets[facet] = {
            'details': {
                'sum_other_doc_count': details['sum_other_doc_count'],
                'doc_count_error_upper_bound': details['doc_count_error_upper_bound'],
            },
            'values': {
                value_details['key']: value_details['doc_count']
                for value_details in details['buckets']
            },
        }

    return facets


# this dict stores cached get_field returns. It is only cleared by restarting the server. This is
# safe because the cached data is keyed on the rounded version and is therefore stable as old
# versions of data can't be modified, so the fields will always be valid. If for some reason this
# isn't the case (such as if redactions for specific fields get added later and old versions of
# records are updated) then the server just needs a restart and that's it).
field_cache = {}


def get_fields(resource_id, version=None):
    """
    Given a resource id, returns the fields that existed at the given version. If the
    version is None then the fields for the latest version are returned.

    The response format is important as it must match the requirements of reclineJS's field
    definitions. See http://okfnlabs.org/recline/docs/models.html#field for more details.

    All fields are returned by default as string types. This is because we have the capability to
    allow searchers to specify whether to treat a field as a string or a number when searching and
    therefore we don't need to try and guess the type and we can leave it to the user to know the
    type which won't cause problems like interpreting a field as a number when it shouldn't be (for
    example a barcode like '013655395'). If we decide that we do want to work out the type we simply
    need to add another step to this function where we count how many records in the version have
    the '.number' subfield - if the number is the same as the normal field count then the field is a
    number type, if not it's a string.

    The fields are returned in either alphabetical order, or if we have the ingestion details for
    the resource at the required version then the order of the fields will match the order of the
    fields in the original source.

    :param resource_id: the resource's id
    :param version: the version of the data we're querying (default: None, which means latest)
    :return: a list of dicts containing the field data
    """
    # figure out the index name from the resource id
    index = prefix_resource(resource_id)
    # figure out the rounded version so that we can figure out the fields at the right version
    rounded_version = common.SEARCH_HELPER.get_rounded_versions([index], version)[index]
    # the key for caching should be unique to the resource and the rounded version
    cache_key = (resource_id, rounded_version)

    # if there is a cached version, return it! Woo!
    if cache_key in field_cache:
        return field_cache[cache_key]

    # create a list of field details, starting with the always present _id field
    fields = [{'id': '_id', 'type': 'integer'}]
    # lookup the mapping on elasticsearch to get all the field names
    mapping = common.ES_CLIENT.indices.get_mapping(index)[index]
    # if the rounded version response is None that means there are no versions available which
    # shouldn't happen, but in case it does for some reason, just return the fields we have
    # already
    if rounded_version is None:
        return mapping, fields

    # retrieve all the resource's details up to the target version to get the column orders at each
    # version as they were in the ingestion sources for each version
    all_details = get_all_details(resource_id, up_to_version=version)
    # this set is used to avoid duplicating fields, we preload it with the _id column because we
    # want to ignore that (it's already in the fields list defined above)
    seen_fields = {'_id'}
    field_names = []

    if all_details:
        # the all_details variable is an OrderedDict in ascending version order. We want to iterate
        # in descending version order though so that we respect the column order at the version
        # we're at before respecting any data from previous versions
        for details in reversed(all_details.values()):
            columns = [
                column for column in details.get_columns() if column not in seen_fields
            ]
            field_names.extend(columns)
            seen_fields.update(columns)

    mapped_fields = mapping['mappings'][DOC_TYPE]['properties']['data']['properties']
    # add any unseen mapped fields to the list of names. If we have a details object for each
    # version this shouldn't add any additional fields and if not it ensures we don't miss any
    field_names.extend(
        field for field in sorted(mapped_fields) if field not in seen_fields
    )

    if field_names:
        # find out which fields exist in this version and how many values each has
        search = MultiSearch(using=common.ES_CLIENT, index=index)
        for field in field_names:
            # create a search which finds the documents that have a value for the given field at the
            # rounded version. We're only interested in the counts though so set size to 0
            search = search.add(
                Search()
                .extra(size=0)
                .filter('exists', **{'field': prefix_field(field)})
                .filter('term', **{'meta.versions': rounded_version})
            )

        # run the search and get the response
        responses = search.execute()
        for i, response in enumerate(responses):
            # if the field has documents then it should be included in the fields list
            if response.hits.total > 0:
                fields.append(
                    {
                        'id': field_names[i],
                        # by default everything is a string
                        'type': 'string',
                    }
                )

    # stick the result in the cache for next time
    field_cache[cache_key] = (mapping, fields)

    return mapping, fields


def convert_to_multisearch(query):
    # save a copy of the original query
    basic_query = copy.deepcopy(query)
    multisearch_query = {}

    # allow other plugins to modify the query before processing, e.g. to remove any
    # custom filters
    for plugin in PluginImplementations(IVersionedDatastore):
        query = plugin.datastore_before_convert_basic_query(query)

    if 'q' in query:
        multisearch_query['search'] = query['q']

    if 'filters' in query:
        filter_list = []
        for field, values in query['filters'].items():
            if not isinstance(values, list):
                values = [values]
            if field == '__geo__':
                for value in values:
                    if isinstance(value, str):
                        value = json.loads(value)
                    if value['type'] == 'Polygon':
                        filter_list.append({'geo_custom_area': [value['coordinates']]})
                    else:
                        # I cannot find any examples of anything other than polygons, so
                        # I'm not sure it was ever implemented for the old searches
                        raise NotImplemented
            else:
                subgroup = []
                subgroup_count = 0
                for value in values:
                    if field != '' and value != '':
                        subgroup.append(
                            {'string_equals': {'fields': [field], 'value': value}}
                        )
                        subgroup_count += 1
                if subgroup_count > 1:
                    filter_list.append({'or': subgroup})
                elif subgroup_count == 1:
                    filter_list += subgroup
        multisearch_query['filters'] = {'and': filter_list}

    # allow plugins to modify the output, with the additional context of the original
    # basic query
    for plugin in PluginImplementations(IVersionedDatastore):
        multisearch_query = plugin.datastore_after_convert_basic_query(
            basic_query, multisearch_query
        )

    return multisearch_query
