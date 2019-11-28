from collections import Counter, defaultdict

from elasticsearch_dsl import MultiSearch, Q
from elasticsearch_dsl.query import Bool

from .utils import chunk_iterator
from .. import common
from ..datastore_utils import prefix_resource, iter_data_fields, unprefix_index, prefix_field
from ..importing.details import get_all_details


def get_mappings(resource_ids, chunk_size=5):
    '''
    Return a dict of mappings for the given resource ids.

    :param resource_ids: a list of resource ids
    :param chunk_size: the number of mappings to get at a time
    :return: a dict of mappings
    '''
    mappings = {}
    for chunk in chunk_iterator(map(prefix_resource, resource_ids), chunk_size):
        mappings.update(common.ES_CLIENT.indices.get_mapping(u','.join(chunk)))
    return mappings


class Fields(object):
    '''
    Class representing the fields from a set of resources. Fields are grouped with other fields that
    have the same name (case-insensitive match) allowing the discovery of fields that are common
    across multiple resources. Note that nested fields only match if the whole case-insensitive path
    matches.
    '''

    def __init__(self, skip_ids=True):
        '''
        :param skip_ids: whether to skip the _id columns or not (at any nesting level)
        '''
        self.skip_ids = skip_ids

        # this counts the number of resources a group appears in
        self.group_counts = Counter()
        # this stores the group names, the fields in each group and the resources each field is
        # included in - i.e. group -> {field -> resource ids}
        self.groups = {}

    def add(self, field_path, resource_id):
        '''
        Adds the given field path from the given resource id into the object's stores.

        :param field_path: a tuple representing a field's path, for root level fields this should be
                           a tuple with a single element like ('genus', ) whereas for nested fields
                           2 or more elements will be present, like ('associatedMedia', 'category').
                           The field path is joined using a '.' to create a full path for each
                           field.
        :param resource_id: the resource id the field belongs to
        '''
        if self.skip_ids and field_path[-1] == u'_id':
            return

        field = u'.'.join(field_path)
        group = field.lower()

        self.group_counts[group] += 1

        if group not in self.groups:
            self.groups[group] = defaultdict(list)
        self.groups[group][field].append(resource_id)

    def top_groups(self):
        '''
        Generator which yields the groups with the highest resource representation from highest to
        lowest.

        :return: a generator which yields a 3-tuple on each iteration. The 3-tuple contains the
                 group name, the resource count and a dict of containing all the field names in the
                 group and the resources they appear in (field name -> list of resource ids)
        '''
        # return a sorted list in reverse count order, secondarily sorted by group name ascending
        # h/t https://stackoverflow.com/a/23033745. We sort by alphabetical secondarily to ensure
        # there is a stable order of these groups
        for group, count in sorted(self.group_counts.most_common(), key=lambda x: (-x[1], x[0])):
            yield group, count, self.groups.get(group, {})


def get_all_fields(resource_ids):
    '''
    Retrieve the fields available across all of the given resources. The Fields object returned
    contains the fields from all versions of the resources.

    :param resource_ids: the resource ids to get the fields from
    :return: a Fields object
    '''
    index_names = [prefix_resource(resource_id) for resource_id in resource_ids]
    mappings = get_mappings(resource_ids)

    fields = Fields()
    for index_name in index_names:
        resource_id = unprefix_index(index_name)
        for field_path, config in iter_data_fields(mappings[index_name]):
            # this catches a corner case where an object type has been specifically defined for a
            # field but no values have been added to the field. Normally, when values exist for a
            # field, we only get the nested field names and not the root name back from
            # iter_data_fields but if no values have been set the object type field will not have
            # any properties and it itself is returned
            if config[u'type'] == u'object':
                continue
            fields.add(field_path, resource_id)

    return fields


def select_fields(fields, search, number_of_groups, ignore_groups):
    '''
    Selects the fields from the given Fields object which are most common across the given
    resource ids. The search parameter is used to limit the records that contribute fields to the
    returned selection. The fields returned must appear in the search in at least one resource with
    at least one value present.

    :param fields: a Fields object
    :param search: an elasticsearch-dsl search object
    :param number_of_groups: the number of groups to select from the Fields object and return
    :param ignore_groups: a set of group names to ignore
    :return: a list of groups, each group is a dict containing:
                - "group" - the group name
                - "count" - the number of resources its fields appear in
                - "records" - the number of records the group's fields appear in
                - "fields" - the fields that make up the group along with the resource ids they come
                             from
    '''
    selected_fields = []
    # make sure we don't get any hits back, we're only interested in the counts
    search = search.extra(size=0)

    def search_iterator():
        for group_name, resource_count, variants in fields.top_groups():
            if group_name in ignore_groups:
                continue

            shoulds = []
            indexes = []
            for variant, resources_in_group in variants.items():
                shoulds.append(Q(u'exists', field=prefix_field(variant)))
                indexes.extend(prefix_resource(resource_id) for resource_id in resources_in_group)

            # yield the group tuple and an elasticsearch-dsl object for the group's fields
            yield (group_name, resource_count, variants), \
                search.index(indexes).filter(Bool(should=shoulds, minimum_should_match=1))

    # iterate over the groups and searches in chunks
    for chunk in chunk_iterator(search_iterator(), chunk_size=number_of_groups):
        groups, searches = zip(*chunk)
        # create a multisearch for all the searches in the group
        multisearch = MultiSearch(using=common.ES_CLIENT)
        for search in searches:
            multisearch = multisearch.add(search)

        for (group, count, fields), response in zip(groups, multisearch.execute()):
            if response.hits.total > 0:
                # a field from this group has values in the search result, add it to the selection
                selected_fields.append(dict(group=group, count=count, records=response.hits.total,
                                            fields=fields))

        if len(selected_fields) >= number_of_groups:
            break

    # sort the returned selected list by count and secondly records
    return sorted(selected_fields, key=lambda s: (s[u'count'], s[u'records']), reverse=True)


def get_single_resource_fields(fields, resource_id, version, search, ignore_groups):
    '''
    Retrieves the fields for a single given resource. The fields are returned in the same format as
    the select_fields function above.

    :param search: an elasticsearch-dsl search object
    :param fields: a Fields object
    :param resource_id: the resource id to be searched in
    :param version: the version we're searching at
    :param search: an elasticsearch-dsl search object
    :param ignore_groups: a set of group names to ignore
    :return: a list of groups, each group is a dict containing:
            - "group" - the group name
            - "count" - the number of resources its fields appear in (will always be 1)
            - "records" - the number of records the group's fields appear in
            - "fields" - the fields that make up the group along with the resource ids they come
                         from (the list of resource ids will always just be the one under search)
    '''
    index = prefix_resource(resource_id)
    # make sure we don't get any hits back, we're only interested in the counts
    search = search.extra(size=0)
    field_names = []
    seen_fields = set()
    selected_fields = []

    # retrieve the datastore resource details if there are any
    all_details = get_all_details(resource_id, up_to_version=version)
    if all_details:
        # the all_details variable is an OrderedDict in ascending version order. We want to iterate
        # in descending version order though so that we respect the column order at the version
        # we're at before respecting any data from previous versions
        for details in reversed(all_details.values()):
            columns = [column for column in details.get_columns() if column not in seen_fields]
            field_names.extend(columns)
            seen_fields.update(columns)

    for _group, _count, fields in fields.top_groups():
        field_names.extend(sorted(field for field in fields.keys() if field not in seen_fields))

    if field_names:
        # remove any fields which shouldn't be included in the response
        field_names = [field for field in field_names if field.lower() not in ignore_groups]

        msearch = MultiSearch(using=common.ES_CLIENT, index=index)
        for field in field_names:
            msearch = msearch.add(search.filter(u'exists', field=prefix_field(field)))

        responses = msearch.execute()
        for i, response in enumerate(responses):
            # if the field has documents then it should be included in the fields list
            if response.hits.total > 0:
                field = field_names[i]
                selected_fields.append(dict(group=field.lower(), count=1,
                                            records=response.hits.total,
                                            fields={field: resource_id}))

    if all_details:
        # if we got a field order from the details in the database use it
        return selected_fields
    else:
        # otherwise, sort the returned selected list by count and secondly records
        return sorted(selected_fields, key=lambda s: (s[u'count'], s[u'records']), reverse=True)
