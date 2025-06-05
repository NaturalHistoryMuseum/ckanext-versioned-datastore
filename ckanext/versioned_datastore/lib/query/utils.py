import json
from copy import deepcopy
from typing import Dict

from ckan.plugins import toolkit
from splitgill.utils import now

from ckanext.versioned_datastore.lib import common
from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.utils import (
    get_available_resources,
    get_database,
    ivds_implementations,
)


def get_resources_and_versions(
    query: SchemaQuery,
    allow_non_datastore: bool = False,
) -> Dict[str, int]:
    """
    Given a query on some resources, returns a dict of resource IDs to versions which
    should be queried based on the overall query version. The version provided with each
    resource ID in the dict will be the rounded version, representing the version of the
    data which should be queried to get the data at the overall version.

    This function also filters out resources the user doesn't have access to.

    :param query: a SchemaQuery object representing the query
    :param allow_non_datastore: allow non datastore resources to be included (will be
        returned with common.NON_DATASTORE_VERSION)
    :returns: a dict of resource IDs and versions
    """
    available_resource_ids = get_available_resources(datastore_only=False)
    if not available_resource_ids.issuperset(query.resource_ids):
        raise toolkit.ValidationError(
            'Not all requested resources are accessible to this user'
        )

    version = now() if query.version is None else query.version
    resource_ids_and_versions = {}
    for resource_id in query.resource_ids:
        # round the version down to ensure we search the exact version requested
        rounded_version = get_database(resource_id).get_rounded_version(version)
        # resource ids without a rounded version are skipped
        if rounded_version is not None:
            resource_ids_and_versions[resource_id] = rounded_version
        elif allow_non_datastore:
            # technically, assuming that a resource with no version available is a
            # non-datastore resource can be wrong because it could just mean that the
            # resource wasn't created at the time of the overall version and the request
            # is before the resource existed in the system at all. However, in practice
            # this isn't worth the hassle of dealing with because it's really unlikely
            # to occur and if it does something funny is happening elsewhere.
            resource_ids_and_versions[resource_id] = common.NON_DATASTORE_VERSION

    return resource_ids_and_versions


def convert_to_multisearch(query: dict) -> dict:
    """
    Converts the given basic query dict into a multisearch query and returns it.

    :param query: a basic query dict
    :returns: a multisearch query dict
    """
    # save a copy of the original query
    basic_query = deepcopy(query)
    multisearch_query = {}

    # allow other plugins to modify the query before processing, e.g. to remove any
    # custom filters
    for plugin in ivds_implementations():
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
    for plugin in ivds_implementations():
        multisearch_query = plugin.datastore_after_convert_basic_query(
            basic_query, multisearch_query
        )

    return multisearch_query


def convert_small_or_groups(query):
    """
    Convert OR groups containing only 1 item to AND groups.

    :param query: a multisearch query dict
    :returns: the query with a converted filter dict, if applicable
    """
    if 'filters' not in query:
        return query

    def _convert(*filters):
        items = []
        for term_or_group in filters:
            k, v = list(term_or_group.items())[0]
            if k not in ['and', 'or', 'not']:
                items.append(term_or_group)
            elif k != 'or' or len(v) != 1:
                # don't convert empty groups because those throw an error for all types
                items.append({k: _convert(*v)})
            else:
                items.append({'and': _convert(*v)})
        return items

    query['filters'] = _convert(query['filters'])[0]

    return query


def remove_empty_groups(query):
    """
    Remove empty groups from filter list.

    :param query: a multisearch query dict
    :returns: the query with a processed filter dict, if applicable
    """
    if 'filters' not in query:
        return query

    def _convert(*filters):
        items = []
        for term_or_group in filters:
            k, v = list(term_or_group.items())[0]
            if k not in ['and', 'or', 'not']:
                items.append(term_or_group)
            elif len(v) > 0:
                items.append({k: _convert(*v)})
        return items

    processed_filters = _convert(query['filters'])
    if len(processed_filters) == 0:
        del query['filters']
    else:
        query['filters'] = processed_filters[0]

    return query
