from collections import defaultdict

from ckanext.versioned_datastore.lib import utils
from eevee.search import create_version_query
from elasticsearch_dsl import Search, MultiSearch


def get_fields(field_counts, ignore_empty_fields, resource_ids=None):
    '''
    Return a sorted list of field names for the resource ids specified using the field counts dict.
    The field counts dict should provide the field names available and their counts at the given
    version for the given search, for each resource in the search. If ignore_empty_fields is True,
    then fields with a count of 0 will be ignored and not returned in the list.

    The list is sorted in ascending order using lowercase comparisons.

    :param field_counts: the dict of resource ids -> fields -> counts
    :param ignore_empty_fields: whether fields with no values should be included in the resulting
                                list or not
    :param resource_ids: the resource ids to get the fields for. The default is None which means
                         that the fields from all resources will be returned
    :return: a list of fields in case-insensitive ascending order
    '''
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
    '''
    Given a download request and an elasticsearch client to work with, work out the number of values
    available per field, per resource for the search.

    :param request: the DownloadRequest object
    :param es_client: the elasticsearch client to use
    :return: a dict of resource ids -> fields -> counts
    '''
    field_counts = defaultdict(dict)
    for resource_id, version in request.resource_ids_and_versions.items():
        index_name = utils.prefix_resource(resource_id)
        # get the base field mapping for the index so that we know which fields to look up, this
        # will get all fields from all versions and therefore isn't usable straight off the bat, we
        # have to then go and see which fields are present in the search at this version
        mapping = es_client.indices.get_mapping(index_name)[index_name]

        # we're going to do a multisearch to find out the number of records a value for each field
        # from the mapping
        search = MultiSearch(using=es_client, index=index_name)
        base_search = Search.from_dict(request.search) \
            .index(index_name) \
            .using(es_client) \
            .extra(size=0) \
            .filter(create_version_query(version))

        # get all the fields names and use dot notation for nested fields
        fields = [u'.'.join(parts) for parts, _config in utils.iter_data_fields(mapping)]
        for field in fields:
            # add a search which finds the documents that have a value for the given field at the
            # right version
            search = search.add(base_search.filter(u'exists', field=utils.prefix_field(field)))

        responses = search.execute()
        for field, response in zip(fields, responses):
            field_counts[resource_id][field] = response.hits.total

    return field_counts
