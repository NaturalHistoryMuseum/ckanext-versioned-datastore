import dataclasses

from splitgill.indexing.fields import ParsedType


class InvalidSortException(Exception):
    def __init__(self, sort: 'Sort'):
        super().__init__(f'Cannot sort on {sort.type} field (field: {sort.field})')


@dataclasses.dataclass
class Sort:
    field: str
    ascending: bool = True
    type: ParsedType = ParsedType.KEYWORD

    @property
    def is_sort_on_id(self) -> bool:
        return self.field == '_id'

    def to_sort(self) -> dict:
        """
        Produce a sort dict for the Elasticsearch request.

        :returns: a dict
        """
        # you're not allowed to sort on text types
        if self.type == ParsedType.TEXT:
            raise InvalidSortException(self)

        unmapped_types = {
            ParsedType.NUMBER: 'double',
            ParsedType.DATE: 'date',
            ParsedType.BOOLEAN: 'boolean',
            ParsedType.KEYWORD: 'keyword',
        }

        options = {
            'order': 'asc' if self.ascending else 'desc',
            'missing': '_last',
            'unmapped_type': unmapped_types[self.type],
        }
        # TODO: do we need this?
        if self.type == ParsedType.DATE:
            options['format'] = 'epoch_millis'
        return {self.type.path_to(self.field, full=True): options}

    @classmethod
    def from_basic(cls, field_and_sort: str) -> 'Sort':
        if field_and_sort.endswith(' desc'):
            return Sort(field_and_sort[:-5], False)
        elif field_and_sort.endswith(' asc'):
            return Sort(field_and_sort[:-4], True)
        return Sort(field_and_sort, True)

    @classmethod
    def from_multi(cls, field: str, order: str, parsed_type: ParsedType) -> 'Sort':
        # only pass ascending=False if the order is desc, otherwise pass True
        return Sort(field, not order == 'desc', parsed_type)

    @classmethod
    def desc(cls, field: str, parsed_type: ParsedType = ParsedType.KEYWORD) -> 'Sort':
        return Sort(field, False, parsed_type)

    @classmethod
    def asc(cls, field: str, parsed_type: ParsedType = ParsedType.KEYWORD) -> 'Sort':
        return Sort(field, True, parsed_type)
