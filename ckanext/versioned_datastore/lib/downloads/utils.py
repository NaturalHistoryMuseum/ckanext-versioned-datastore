from elasticsearch_dsl import Search, A
from fastavro import parse_schema
from splitgill.search import create_version_query

from .query import Query
from .. import common
from ..datastore_utils import (
    prefix_resource,
    prefix_field,
    iter_data_fields,
    unprefix_index,
)
from ..query.fields import get_mappings


def get_schemas(query: Query):
    """
    Creates an avro schema from the elasticsearch index metadata.

    :param query: the Query object for this request
    :return: a parsed avro schema
    """
    # get the mappings for the resources which would have a mapping (i.e. exclude
    # non-datastore resources)
    resource_mapping = get_mappings(
        [
            resource_id
            for resource_id, version in query.resource_ids_and_versions.items()
            if version != common.NON_DATASTORE_VERSION
        ]
    )

    basic_avro_types = [
        'null',
        'boolean',
        'int',
        'long',
        'float',
        'double',
        'bytes',
        'string',
    ]
    avro_types = basic_avro_types + [
        {'type': 'array', 'items': basic_avro_types.copy()}
    ]
    avro_map_type = {'type': 'map', 'values': avro_types.copy()}
    object_avro_types = [
        'null',
        avro_map_type,
        {'type': 'array', 'items': avro_map_type.copy()},
    ]

    def _get_nest_level(field_data, nest_level=1):
        if 'properties' in field_data:
            return max(
                [
                    _get_nest_level(fd, nest_level + 1)
                    for fd in field_data['properties'].values()
                ]
            )
        if field_data['type'] == 'object':
            # 20 is the default nesting limit for ES but that makes things so slow that
            # it breaks, so we'll just hope that our data have max 5 levels
            # https://www.elastic.co/guide/en/elasticsearch/reference/master/mapping-settings-limit.html
            return 5
        else:
            return nest_level

    resource_schemas = {}
    for prefixed_resource, mapping in resource_mapping.items():
        schema_fields = {}
        field_list = mapping['mappings']['_doc']['properties']['data']['properties']
        for field_name, type_dict in field_list.items():
            max_nest_level = _get_nest_level(type_dict)
            if max_nest_level > 1:
                nested_type = object_avro_types.copy()
                for level in range(max_nest_level):
                    nested_type += [
                        {'type': 'array', 'items': nested_type.copy()},
                        {'type': 'map', 'values': nested_type.copy()},
                    ]
                schema_fields[field_name] = {'name': field_name, 'type': nested_type}
            else:
                schema_fields[field_name] = {'name': field_name, 'type': avro_types}
        schema_json = {
            'type': 'record',
            'name': 'ResourceRecord',
            'fields': list(schema_fields.values()),
        }
        resource_schemas[unprefix_index(prefixed_resource)] = parse_schema(schema_json)

    return resource_schemas


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
    :return: a list of fields in case-insensitive ascending order
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


def calculate_field_counts(query, es_client, resource_id, resource_version):
    """
    Given a download request and an elasticsearch client to work with, work out the
    number of values available per field, per resource for the search.

    :param query: the Query object
    :param es_client: the elasticsearch client to use
    :param resource_id:
    :param resource_version:
    :return: a dict of resource ids -> fields -> counts
    """
    field_counts = {}
    index_name = prefix_resource(resource_id)
    # get the base field mapping for the index so that we know which fields to look up,
    # this will get all fields from all versions and therefore isn't usable straight off
    # the bat, we have to then go and see which fields are present in the search at this
    # version
    mapping = es_client.indices.get_mapping(index_name)[index_name]

    search = (
        Search.from_dict(query.search.to_dict())
        .index(index_name)
        .using(es_client)
        .extra(size=0)
        .filter(create_version_query(resource_version))
    )

    # get all the fields names and use dot notation for nested fields
    fields = ['.'.join(parts) for parts, _config in iter_data_fields(mapping)]
    for field in fields:
        # add a search which finds the documents that have a value for the given field
        # at the right version
        agg = A('value_count', field=prefix_field(field))
        search.aggs.bucket(field, agg)

    response = search.execute()
    for field in fields:
        field_counts[field] = response.aggregations[field].value

    return field_counts


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
    :return: a new dict containing only the fields from the original data dict that had
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
    :return: the flattened dict
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
