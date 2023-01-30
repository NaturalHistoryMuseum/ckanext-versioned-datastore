import itertools
from collections import Counter, defaultdict

import re
from elasticsearch_dsl import MultiSearch, Q
from elasticsearch_dsl.query import Bool

from .utils import chunk_iterator
from .. import common
from ..datastore_utils import (
    prefix_resource,
    iter_data_fields,
    unprefix_index,
    prefix_field,
)
from ..importing.details import get_all_details


def get_mappings(resource_ids, chunk_size=5):
    """
    Return a dict of mappings for the given resource ids.

    :param resource_ids: a list of resource ids
    :param chunk_size: the number of mappings to get at a time
    :return: a dict of mappings
    """
    mappings = {}
    for chunk in chunk_iterator(map(prefix_resource, resource_ids), chunk_size):
        mappings.update(common.ES_CLIENT.indices.get_mapping(','.join(chunk)))
    return mappings


class Fields(object):
    """
    Class representing the fields from a set of resources.

    Fields are grouped with other fields that have the same name (case-insensitive
    match) allowing the discovery of fields that are common across multiple resources.
    Note that nested fields only match if the whole case-insensitive path matches.
    """

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
        self.ignore_groups = set()
        self.force_groups = []

    def add(self, field_path, resource_id):
        """
        Adds the given field path from the given resource id into the object's stores.

        :param field_path: a tuple representing a field's path, for root level fields this should be
                           a tuple with a single element like ('genus', ) whereas for nested fields
                           2 or more elements will be present, like ('associatedMedia', 'category').
                           The field path is joined using a '.' to create a full path for each
                           field.
        :param resource_id: the resource id the field belongs to
        """
        if self.skip_ids and field_path[-1] == '_id':
            return

        field = '.'.join(field_path)
        group = field.lower()

        self.group_counts[group] += 1

        if group not in self.groups:
            self.groups[group] = defaultdict(list)
        self.groups[group][field].append(resource_id)

    def ignore(self, group):
        if hasattr(group, 'match') and callable(group.match):
            self.ignore_groups.add(group)
        else:
            self.ignore_groups.add(re.compile(group, re.IGNORECASE))

    def force(self, group):
        if group not in self.force_groups:
            self.force_groups.append(group.lower())

    def get_forced_groups(self):
        for group in self.force_groups:
            if group in self.group_counts:
                yield group, self.group_counts[group], self.groups.get(group, {})

    def top_groups(self):
        """
        Generator which yields the groups with the highest resource representation from
        highest to lowest.

        :return: a generator which yields a 3-tuple on each iteration. The 3-tuple contains the
                 group name, the resource count and a dict of containing all the field names in the
                 group and the resources they appear in (field name -> list of resource ids)
        """
        # return a sorted list in reverse count order, secondarily sorted by group name ascending
        # h/t https://stackoverflow.com/a/23033745. We sort by alphabetical secondarily to ensure
        # there is a stable order of these groups
        for group, count in sorted(
            self.group_counts.most_common(), key=lambda x: (-x[1], x[0])
        ):
            # if the group is in the force_groups list then skip it
            if group in self.force_groups:
                continue

            # ignore groups that have been specifically ignored
            if any(ignore.match(group) for ignore in self.ignore_groups):
                continue

            yield group, count, self.groups.get(group, {})

    def get_searches(self, search):
        groups = itertools.chain(self.get_forced_groups(), self.top_groups())
        for group_name, resource_count, variants in groups:
            shoulds = []
            indexes = []
            for variant, resources_in_group in variants.items():
                shoulds.append(Q('exists', field=prefix_field(variant)))
                indexes.extend(
                    prefix_resource(resource_id) for resource_id in resources_in_group
                )

            # yield the group tuple and an elasticsearch-dsl object for the group's fields
            yield (group_name, resource_count, variants), search.index(indexes).filter(
                Bool(should=shoulds, minimum_should_match=1)
            )

    def is_forced(self, group):
        return group in self.force_groups


def get_all_fields(resource_ids):
    """
    Retrieve the fields available across all of the given resources. The Fields object
    returned contains the fields from all versions of the resources.

    :param resource_ids: the resource ids to get the fields from
    :return: a Fields object
    """
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
            if config['type'] == 'object':
                continue
            fields.add(field_path, resource_id)

    return fields


def select_fields(all_fields, search, number_of_groups):
    """
    Selects the fields from the given Fields object which are most common across the
    given resource ids. The search parameter is used to limit the records that
    contribute fields to the returned selection. The fields returned must appear in the
    search in at least one resource with at least one value present.

    :param all_fields: a Fields object
    :param search: an elasticsearch-dsl search object
    :param number_of_groups: the number of groups to select from the Fields object and return
    :return: a list of groups, each group is a dict containing:
                - "group" - the group name
                - "count" - the number of resources its fields appear in
                - "records" - the number of records the group's fields appear in
                - "fields" - the fields that make up the group along with the resource ids they come
                             from
                - "forced" - whether the field was forced into being included, or whether it was
                             included organically
    """
    selected_fields = []
    # make sure we don't get any hits back, we're only interested in the counts
    search = search.extra(size=0)

    # iterate over the groups and searches in chunks
    for chunk in chunk_iterator(
        all_fields.get_searches(search), chunk_size=number_of_groups
    ):
        groups, searches = zip(*chunk)
        # create a multisearch for all the searches in the group
        multisearch = MultiSearch(using=common.ES_CLIENT)
        for search in searches:
            multisearch = multisearch.add(search)

        for (group, count, fields), response in zip(groups, multisearch.execute()):
            if all_fields.is_forced(group) or response.hits.total > 0:
                # a field from this group has values in the search result, add it to the selection
                selected_fields.append(
                    dict(
                        group=group,
                        count=count,
                        records=response.hits.total,
                        fields=fields,
                        forced=all_fields.is_forced(group),
                    )
                )

        if len(selected_fields) >= number_of_groups:
            break

    def group_sorter(the_group):
        # this sorts the groups ensuring forced groups are first, in the order they were forced,
        # then the groups with highest count and then the ones with the highest number of records
        if the_group['forced']:
            # use 0 0 to ensure that the base order of the groups is maintained for forced groups
            return True, 0, 0
        else:
            return False, the_group['count'], the_group['records']

    # sort the returned selected list by count and secondly records
    return sorted(selected_fields, key=group_sorter, reverse=True)


def get_single_resource_fields(
    all_fields, resource_id, version, search, number_of_groups
):
    """
    Retrieves the fields for a single given resource. The fields are returned in the
    same format as the select_fields function above.

    :param all_fields: a Fields object
    :param resource_id: the resource id to be searched in
    :param version: the version we're searching at
    :param search: an elasticsearch-dsl search object
    :param number_of_groups: the maximum number of groups to return
    :return: a list of groups, each group is a dict containing:
            - "group" - the group name
            - "count" - the number of resources its fields appear in (will always be 1)
            - "records" - the number of records the group's fields appear in
            - "fields" - the fields that make up the group along with the resource ids they come
                         from (the list of resource ids will always just be the one under search)
    """
    # retrieve the datastore resource details if there are any
    all_details = get_all_details(resource_id, up_to_version=version)
    if all_details:
        # the all_details variable is an OrderedDict in ascending version order. We want to iterate
        # in descending version order though so that we respect the column order at the version
        # we're at before respecting any data from previous versions
        for details in reversed(all_details.values()):
            for field_name in details.get_columns():
                all_fields.force(field_name)

    return select_fields(all_fields, search, number_of_groups)
