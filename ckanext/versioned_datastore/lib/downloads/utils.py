from typing import Dict, List, Optional, Union

from splitgill.indexing.fields import DataField

from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.utils import get_database


def _get_field_type(field: DataField) -> List[Union[str, dict]]:
    """
    Given a field, get its Avro type information. If the field is a complex type (list,
    dict) this function will recurse to create type information for the field and its
    children.

    :param field: the field to create schema for
    :returns: the schema for the field as
    """
    types = []

    # check all basic types except nulls (they are always added last)
    if field.is_str:
        types.append('string')
    if field.is_int:
        types.append('long')
    if field.is_float:
        types.append('double')
    if field.is_bool:
        types.append('boolean')

    # now check the complex types, first lists
    if field.is_list:
        types.append(
            {
                'type': 'array',
                'items': [
                    _get_field_type(child)
                    for child in field.children
                    if child.is_list_element
                ],
            }
        )

    # then check dicts
    if field.is_dict:
        types.append(
            {
                'type': 'record',
                'name': f'{field.path}Record',
                'fields': [
                    {
                        'name': child.name,
                        'type': _get_field_type(child),
                    }
                    for child in field.children
                    if not child.is_list_element
                ],
            }
        )

    # ensure all types are nullable
    types.append('null')
    return types


def get_schema(
    resource_id: str, version: Optional[int] = None, query: Optional[SchemaQuery] = None
) -> dict:
    """
    Creates an Avro schema for the given resource at the given version for the records
    found by the given query.

    :param resource_id: the resource ID
    :param version: the version
    :param query: the query
    :returns: an Avro schema as a dict
    """
    database = get_database(resource_id)
    fields = database.get_data_fields(version, query.to_dsl() if query else None)
    schema = {
        'type': 'record',
        'name': 'Record',
        'fields': [
            {
                'name': field.name,
                'type': _get_field_type(field),
            }
            for field in fields
            if field.is_root_field
        ],
    }
    return schema


def get_fields(field_counts, ignore_empty_fields, resource_id=None):
    """
    Return a sorted list of field names for the resource ids specified using the field
    counts dict. The field counts dict should provide the field names available and
    their counts at the given version for the given search, for each resource in the
    search. If ignore_empty_fields is True, then fields with a count of 0 will be
    ignored and not returned in the list.

    The list is sorted in ascending order using lowercase comparisons.

    :param field_counts: the dict of resource ids -> fields -> counts
    :param ignore_empty_fields: whether fields with no values should be included in the
        resulting list or not
    :param resource_id: the resource id to get the fields for. The default is None which
        means that the fields from all resources will be returned
    :returns: a list of fields in case-insensitive ascending order
    """
    # TODO: retrieve the sort order for resources from the database and use
    field_names = set()

    if resource_id:
        field_counts_list = [field_counts[resource_id]]
    else:
        field_counts_list = list(field_counts.values())

    for counts in field_counts_list:
        for field, count in counts.items():
            if count == 0 and ignore_empty_fields:
                continue
            else:
                field_names.add(field)

    return sorted(field_names, key=lambda f: f.lower())


def calculate_field_counts(
    resource_id: str, version: int, query: SchemaQuery
) -> Dict[str, int]:
    """
    Given a download request and an elasticsearch client to work with, work out the
    number of values available per field, per resource for the search.

    :param query: the Query object
    :param resource_id:
    :param version:
    :returns: a dict of fields -> counts
    """
    database = get_database(resource_id)
    # todo: depending on what this is used for, should this be using get_data_fields?
    # grab all the fields at this version
    all_fields = database.get_parsed_fields(version)
    # grab the fields that appear in this specific query's results
    found_fields = {
        field.path for field in database.get_parsed_fields(version, query.to_dsl())
    }
    return {
        field.path: field.count if field.path in found_fields else 0
        for field in all_fields
    }


def filter_data_fields(data, field_counts, prefix=None):
    """
    Returns a new dict containing only the keys and values from the given data dict
    where the corresponding field in the field_counts dict has a value greater than 0 -
    i.e. removes all fields from the data dict that shouldn't be included.

    Note that this may seem like a pointless exercise as surely if the field count is 0
    for a field then it won't appear in any of the data dicts - however, because the
    data returned from elasticsearch is the source dict that was uploaded to it, it
    could contain nulls and empty string values. The calculate_field_counts function
    above that generates the field counts does it by using exist queries and because
    these count indexed values it skips nulls and empty strings.

    :param data: the data dict
    :param field_counts: a dict of field names and counts, the field names should be dot
                         separated for nested fields
    :param prefix: the prefix under which the fields in the passed data dict exist -
                   this is used to produce the field names for nested fields
    :returns: a new dict containing only the fields from the original data dict that had
             a value other than 0 in the fields_count dict
    """
    filtered_data = {}
    for field, value in data.items():
        if prefix is not None:
            path = f'{prefix}.{field}'
        else:
            path = field

        # if the field contains a list of dicts we need to recurse for each one
        if isinstance(value, list) and value and isinstance(value[0], dict):
            filtered_value = []
            for element in value:
                filtered_element = filter_data_fields(
                    element, field_counts, prefix=path
                )
                # if there is any data left in the element after filtering, add it to
                # the temp list
                if filtered_element:
                    filtered_value.append(filtered_element)
            # if there are any dicts left from the filtering, include them directly
            # using the name of the field, not the path. We don't need to check if the
            # field has any values because we know that it does because there are dicts
            # left in the filtered list
            if filtered_value:
                filtered_data[field] = filtered_value
        # if the field is a dict, recurse to filter
        elif isinstance(value, dict):
            filtered_value = filter_data_fields(value, field_counts, prefix=path)
            # if there is any data left after the filtering, include the dict value
            # directly, using the name of the field, not the path. We don't need to
            # check if the field has any values because we know that it does because the
            # filtered_value isn't empty
            if filtered_value:
                filtered_data[field] = filtered_value
        # for everything else, just check that the path is in the fields_count dict
        elif field_counts.get(path, 0):
            filtered_data[field] = value

    return filtered_data


def flatten_dict(data, path=None, separator=' | '):
    """
    Flattens a given dictionary so that nested dicts and lists of dicts are available
    from the root of the dict. For nested dicts, the keys in the nested dict are simply
    concatenated to the key that references the dict with a dot between each, for
    example:

        {"a": {"b": 4, "c": 6}} -> {"a.b": 4, "a.c": 6}
    This works to any nesting level.
    For lists of dicts, the common keys between them are pulled up to the level above
    in the same way as the standard nested dict, but if there are multiple dicts with
    the same keys the values associated with them are concatenated together using the
    separator parameter. For example:
        {"a": [{"b": 5}, {"b": 19}]} -> {"a.b": "5 | 19"}
    :param data: the dict to flatten
    :param path: the path to place all found keys under, by default this is None and
                 therefore the keys in the dict are not placed under anything and are
                 used as is. This is really only here for internal recursive purposes.
    :param separator: the string to use when concatenating lists of values, whether
                      common ones from a list of dicts, or indeed just a normal list of
                      values
    :returns: the flattened dict
    """
    flat = {}
    for key, value in data.items():
        if path is not None:
            # use a dot to indicate this key is below the parent in the path
            key = f'{path}.{key}'

        if isinstance(value, dict):
            # flatten the nested dict then update the current dict we've got on the go
            flat.update(flatten_dict(value, path=key))
        elif isinstance(value, list):
            if all(isinstance(element, dict) for element in value):
                for element in value:
                    # iterate through the list of dicts flattening each as we go and
                    # then either just adding the value to the dict we've got on the go
                    # or appending it to the string value we're using for collecting
                    # multiples
                    for subkey, subvalue in flatten_dict(element, path=key).items():
                        if subkey not in flat:
                            flat[subkey] = subvalue
                        else:
                            flat[subkey] = f'{flat[subkey]}{separator}{subvalue}'
            else:
                flat[key] = separator.join(map(str, value))
        else:
            flat[key] = value

    return flat
