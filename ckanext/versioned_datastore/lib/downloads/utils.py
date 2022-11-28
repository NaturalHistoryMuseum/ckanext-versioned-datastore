from collections import defaultdict

from splitgill.search import create_version_query
from elasticsearch_dsl import Search, A

from ..datastore_utils import prefix_resource, prefix_field, iter_data_fields


def get_fields(field_counts, ignore_empty_fields, resource_ids=None):
    """
    Return a sorted list of field names for the resource ids specified using the field
    counts dict. The field counts dict should provide the field names available and
    their counts at the given version for the given search, for each resource in the
    search. If ignore_empty_fields is True, then fields with a count of 0 will be
    ignored and not returned in the list.

    The list is sorted in ascending order using lowercase comparisons.

    :param field_counts: the dict of resource ids -> fields -> counts
    :param ignore_empty_fields: whether fields with no values should be included in the resulting
                                list or not
    :param resource_ids: the resource ids to get the fields for. The default is None which means
                         that the fields from all resources will be returned
    :return: a list of fields in case-insensitive ascending order
    """
    # TODO: retrieve the sort order for resources from the database and use
    field_names = set()

    for resource_id, counts in field_counts.items():
        if resource_ids is None or resource_id in resource_ids:
            for field, count in counts.items():
                if count == 0 and ignore_empty_fields:
                    continue
                else:
                    field_names.add(field)

    return sorted(field_names, key=lambda f: f.lower())


def calculate_field_counts(request, es_client):
    """
    Given a download request and an elasticsearch client to work with, work out the
    number of values available per field, per resource for the search.

    :param request: the DownloadRequest object
    :param es_client: the elasticsearch client to use
    :return: a dict of resource ids -> fields -> counts
    """
    field_counts = defaultdict(dict)
    for resource_id, version in request.resource_ids_and_versions.items():
        index_name = prefix_resource(resource_id)
        # get the base field mapping for the index so that we know which fields to look up, this
        # will get all fields from all versions and therefore isn't usable straight off the bat, we
        # have to then go and see which fields are present in the search at this version
        mapping = es_client.indices.get_mapping(index_name)[index_name]

        search = (
            Search.from_dict(request.search)
            .index(index_name)
            .using(es_client)
            .extra(size=0)
            .filter(create_version_query(version))
        )

        # get all the fields names and use dot notation for nested fields
        fields = ['.'.join(parts) for parts, _config in iter_data_fields(mapping)]
        for field in fields:
            # add a search which finds the documents that have a value for the given field at the
            # right version
            agg = A('value_count', field=prefix_field(field))
            search.aggs.bucket(field, agg)

        response = search.execute()
        for field in fields:
            field_counts[resource_id][field] = response.aggregations[field].value

    return field_counts


def filter_data_fields(data, field_counts, prefix=None):
    """
    Returns a new dict containing only the keys and values from the given data dict
    where the.

    corresponding field in the field_counts dict has a value greater than 0 - i.e. removes all
    fields from the data dict that shouldn't be included.

    Note that this may seem like a pointless exercise as surely if the field count is 0 for a field
    then it won't appear in any of the data dicts - however, because the data returned from
    elasticsearch is the source dict that was uploaded to it, it could contain nulls and empty
    string values. The calculate_field_counts function above that generates the field counts does it
    by using exist queries and because these count indexed values it skips nulls and empty strings.

    :param data: the data dict
    :param field_counts: a dict of field names and counts, the field names should be dot separated
                         for nested fields
    :param prefix: the prefix under which the fields in the passed data dict exist - this is used to
                   produce the field names for nested fields
    :return: a new dict containing only the fields from the original data dict that had a value
             other than 0 in the fields_count dict
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
                # if there is any data left in the element after filtering, add it to the temp list
                if filtered_element:
                    filtered_value.append(filtered_element)
            # if there are any dicts left from the filtering, include them directly using the name
            # of the field, not the path. We don't need to check if the field has any values because
            # we know that it does because there are dicts left in the filtered list
            if filtered_value:
                filtered_data[field] = filtered_value
        # if the field is a dict, recurse to filter
        elif isinstance(value, dict):
            filtered_value = filter_data_fields(value, field_counts, prefix=path)
            # if there is any data left after the filtering, include the dict value directly, using
            # the name of the field, not the path. We don't need to check if the field has any
            # values because we know that it does because the filtered_value isn't empty
            if filtered_value:
                filtered_data[field] = filtered_value
        # for everything else, just check that the path is in the fields_count dict
        elif field_counts.get(path, 0):
            filtered_data[field] = value

    return filtered_data
