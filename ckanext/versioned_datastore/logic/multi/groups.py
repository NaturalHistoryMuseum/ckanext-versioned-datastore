import re
from collections import defaultdict
from functools import total_ordering
from itertools import chain, islice
from typing import Dict, List, Set, Tuple, Union

from splitgill.indexing.fields import ParsedField

from ckanext.versioned_datastore.lib.importing.details import get_all_details


@total_ordering
class Group:
    def __init__(self, name: str):
        self.name: str = name
        self.fields: List[Tuple[ParsedField, str]] = []

    @property
    def resource_count(self) -> int:
        return len({resource_id for _, resource_id in self.fields})

    @property
    def record_count(self) -> int:
        # todo: this was sum for a bit, check this ok
        return max(field.count for field, _ in self.fields)

    @property
    def path_to_resource_id_map(self) -> Dict[str, List[str]]:
        result = defaultdict(set)
        for field, resource_id in self.fields:
            result[field.path].add(resource_id)
        return {field: list(resource_ids) for field, resource_ids in result.items()}

    def add(self, resource_id: str, field: ParsedField):
        self.fields.append((field, resource_id))

    def __eq__(self, other) -> bool:
        if isinstance(other, Group):
            return other.name == self.name
        return NotImplemented

    def __lt__(self, other) -> bool:
        if isinstance(other, Group):
            return (self.resource_count, self.record_count, self.name) < (
                other.resource_count,
                other.record_count,
                other.name,
            )
        return NotImplemented


class FieldGroups:
    def __init__(self, skip_ids: bool = True):
        self.skip_ids = skip_ids
        self.ignore_groups: Set[re.Pattern] = set()
        self.force_groups: List[str] = []
        self.groups: Dict[str, Group] = {}

    def ignore(self, group: Union[re.Pattern, str]):
        if isinstance(group, re.Pattern):
            self.ignore_groups.add(group)
        else:
            self.ignore_groups.add(re.compile(group, re.IGNORECASE))

    def force(self, group: str):
        if group not in self.force_groups:
            self.force_groups.append(group.lower())

    def add(self, resource_id: str, fields: List[ParsedField]):
        for field in fields:
            group = field.path.lower()
            self.groups.setdefault(group, Group(group)).add(resource_id, field)

    def force_resource_fields(self, resource_id, version):
        # retrieve the datastore resource details if there are any
        all_details = get_all_details(resource_id, up_to_version=version)
        if all_details:
            # the all_details variable is an OrderedDict in ascending version order. We
            # want to iterate in descending version order though so that we respect the
            # column order at the version we're at before respecting any data from
            # previous versions
            for details in reversed(all_details.values()):
                for field_name in details.get_columns():
                    self.force(field_name)

    def select(self, count: int) -> List[dict]:
        groups = []
        forced = [
            self.groups[group] for group in self.force_groups if group in self.groups
        ]
        for group in sorted(self.groups.values(), reverse=True):
            # process skips and ignores
            if self.skip_ids and group.name == '_id':
                continue
            if any(ignore.match(group.name) for ignore in self.ignore_groups):
                continue
            # skip these, we'll add them later
            if group in forced:
                continue
            groups.append(group)

        return [
            {
                'group': group.name,
                'count': group.resource_count,
                'records': group.record_count,
                'fields': group.path_to_resource_id_map,
                'forced': group in forced,
            }
            for group in islice(chain(forced, groups), count)
        ]
