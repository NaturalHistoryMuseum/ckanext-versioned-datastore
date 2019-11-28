from collections import Counter, defaultdict

from elasticsearch_dsl import MultiSearch, Q
from elasticsearch_dsl.query import Bool

from .. import common
from ..datastore_utils import prefix_resource, iter_data_fields, unprefix_index, prefix_field
from ..importing.details import get_all_details
from .utils import chunk_iterator


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
        # h/t https://stackoverflow.com/a/23033745
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
    mappings = {}
    for chunk in chunk_iterator(index_names, 5):
        mappings.update(common.ES_CLIENT.indices.get_mapping(u','.join(chunk)))

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


def select_fields(fields, search, resource_ids, number_of_groups):
    '''
    Selects the fields from the given Fields object which are most common across the given
    resource ids. The search parameter is used to limit the records that contribute fields to the
    returned selection. The fields returned must appear in the search in at least one resource with
    at least one value present.

    :param fields: a Fields object
    :param search: an elasticsearch-dsl search object
    :param resource_ids: the resource ids to be searched in
    :param number_of_groups: the number of groups to select from the Fields object and return
    :return: a list of groups, each group is a dict containing the group name, the number of
             resources its fields appear in and the fields that make up the group along with the
             resource ids they come from
    '''
    selected_fields = []
    # make sure we don't get any hits back, we're only interested in the counts
    search = search.extra(size=0)
    for group_chunk in chunk_iterator(fields.top_groups(), chunk_size=number_of_groups):
        # we're going to build a multisearch which will contain a query per group as by sending them
        # all to elasticsearch in one shot we gain some efficiency
        multisearch = MultiSearch(using=common.ES_CLIENT)
        for group, count, fields in group_chunk:
            shoulds = []
            indexes = []
            for variant, resources_in_group in fields.items():
                shoulds.append(Q(u'exists', field=prefix_field(variant)))
                indexes.extend(prefix_resource(resource_id) for resource_id in resources_in_group)

            multisearch = multisearch.add(search.index(indexes)
                                          .filter(Bool(should=shoulds, minimum_should_match=1)))

        for (group, count, fields), response in zip(group_chunk, multisearch.execute()):
            if response.hits.total > 0:
                # a field from this group has values in the search result, add it to the selection
                selected_fields.append(dict(group=group, count=count, fields=fields))

        if len(selected_fields) >= number_of_groups:
            break

    return selected_fields


def get_single_resource_fields(fields, resource_id, version, search):
    '''
    Retrieves the fields for a single given resource. The fields are returned in the same format as
    the select_fields function above.

    :param search: an elasticsearch-dsl search object
    :param fields: a Fields object
    :param resource_id: the resource id to be searched in
    :param version: the version we're searching at
    :param search: an elasticsearch-dsl search object
    :return: a list of groups, each group is a dict containing the group name, the number of
             resources its fields appear in and the fields that make up the group along with the
             resource ids they come from
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
        msearch = MultiSearch(using=common.ES_CLIENT, index=index)
        for field in field_names:
            msearch = msearch.add(search.filter(u'exists', field=prefix_field(field)))

        responses = msearch.execute()
        for i, response in enumerate(responses):
            # if the field has documents then it should be included in the fields list
            if response.hits.total > 0:
                field = field_names[i]
                selected_fields.append(dict(group=field.lower(), count=1,
                                            fields={field: resource_id}))

    return selected_fields
