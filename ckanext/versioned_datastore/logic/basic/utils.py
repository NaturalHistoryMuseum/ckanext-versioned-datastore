from operator import itemgetter
from typing import Dict, List, Optional

from splitgill.indexing.fields import DataField, ParsedField, ParsedType
from splitgill.search import keyword

from ckanext.versioned_datastore.lib.importing.details import get_details_at
from ckanext.versioned_datastore.lib.query.search.query import BasicQuery
from ckanext.versioned_datastore.lib.query.search.request import SearchRequest
from ckanext.versioned_datastore.lib.query.search.sort import Sort
from ckanext.versioned_datastore.lib.utils import get_database


def find_version(data_dict: dict) -> Optional[int]:
    """
    Retrieve the version from the data_dict. The version can be specified as a parameter
    in its own right or as a special filter in the filters dict using the key
    __version__. Using the version parameter is preferred and will override any filter
    version value. The filter method is provided because of limitations in the CKAN
    recline.js framework used by the NHM on CKAN 2.3 where no additional parameters can
    be passed other than q, filters etc.

    :param data_dict: the data dict, this might be modified if the __version__ key is
        used (it will be removed if present)
    :returns: the version found as an integer, or None if no version was found
    """
    version = data_dict.get('version')
    # pop the __version__ to avoid including it in the normal search filters, even if we
    # don't use it
    filter_version = data_dict.get('filters', {}).pop('__version__', None)

    if version is not None:
        return int(version)

    if filter_version is not None:
        # it'll probably be a list because it's a normal filter as far as the frontend
        # is concerned
        if isinstance(filter_version, list):
            # just use the first value
            filter_version = filter_version[0]
        if filter_version is not None:
            return int(filter_version)

    # no version found, return None
    return None


def make_request(data_dict: dict) -> SearchRequest:
    """
    Creates a SearchRequest from the given data_dict and returns it. The SearchRequest
    object will use a BasicQuery as its query which will be made by this function from
    the given data_dict. This function should be used by all basic actions which need to
    use basic queries for searches.

    :param data_dict: the data_dict passed to the action
    :returns: a SearchRequest object
    """
    query = BasicQuery(
        data_dict['resource_id'],
        find_version(data_dict),
        data_dict.get('q'),
        data_dict.get('filters'),
    )
    request = SearchRequest(
        query,
        size=data_dict.get('limit'),
        offset=data_dict.get('offset'),
        after=data_dict.get('after'),
        sorts=list(map(Sort.from_basic, data_dict.get('sort', []))),
        fields=data_dict.get('fields', []),
        data_dict=data_dict,
    )
    if 'facets' in data_dict:
        facet_limits = data_dict.get('facet_limits', {})
        for facet in data_dict['facets']:
            request.add_agg(
                facet,
                'terms',
                field=keyword(facet),
                size=facet_limits.get(facet, 10),
            )
    return request


def format_facets(aggs: dict) -> dict:
    """
    Formats the facet aggregation result into the format we require. Specifically we
    expand the buckets out into a dict that looks like this:

        {
            "facet1": {
                "details": {
                    "sum_other_doc_count": 34,
                    "doc_count_error_upper_bound": 3
                },
                "values": {
                    "value1": 1,
                    "value2": 4,
                    "value3": 1,
                    "value4": 2,
                }
            },
            etc
        }

    etc.

    :param aggs: the aggregation dict returned from splitgill/elasticsearch
    :returns: the facet information as a dict
    """
    facets = {}
    for facet, details in aggs.items():
        facets[facet] = {
            'details': {
                'sum_other_doc_count': details['sum_other_doc_count'],
                'doc_count_error_upper_bound': details['doc_count_error_upper_bound'],
            },
            'values': {
                value_details['key']: value_details['doc_count']
                for value_details in details['buckets']
            },
        }

    return facets


def infer_type(
    data_field: DataField, parsed_fields: Dict[str, ParsedField], threshold: float = 0.8
) -> str:
    """
    Infers the type of each of the given data fields, using the parsed fields to
    determine what type to assign. A type is assigned to a field if more than the
    threshold percentage of values in the field are of the parsed type.

    This function uses the parsed types instead of the data types for three reasons:
      - often the field data types are just strings, so we'd need to use the parsed
        types anyway to get a non-string type
      - the response to the get_fields function where this function is called is
        typically used in a search result and therefore the fields response may be used
        to create another search in which case the parsed types would have to be used
        as you can't search using the data types
      - there is no date field data type, whereas there is a date field parsed type

    In most cases, the parsed type returned by this function will match the data type of
    the field's values.

    :param data_field: the data field to infer for
    :param parsed_fields: a dict of parsed paths to parsed fields
    :param threshold: a threshold determining the percentage of the values in the field
                      which must be of the parsed type to be inferred as it
                      (default: 0.8, i.e., 80%)
    :returns: the name of the inferred type (one of string, number, date, boolean)
    """
    parsed_field = parsed_fields.get(data_field.parsed_path)

    # this should only ever happen for fields which are always nested objects
    if not parsed_field:
        return 'object'

    # check for dates first, then booleans, then numbers, and then default to string
    for parsed_type in (ParsedType.DATE, ParsedType.BOOLEAN, ParsedType.NUMBER):
        if parsed_field.type_counts[parsed_type] / parsed_field.count >= threshold:
            # just so happens that the recline types match the ParsedType names
            return parsed_type.name.lower()
    return 'string'


def get_fields(resource_id: str, version: Optional[int] = None) -> List[dict]:
    """
    Given a resource id, returns the fields that existed at the given version. If the
    version is None then the fields for the latest version are returned.

    The response format is important as it must match the requirements of reclineJS's
    field definitions. See
    http://okfnlabs.org/recline/docs/models.html#field
     for more    details.

    All fields are returned by default as string or array types. This is because we have
    the capability to allow searchers to specify whether to treat a field as other types
    when searching, and therefore we don't need to try and guess the type, and we can
    leave it to the user to know the type which won't cause problems like interpreting a
    field as a number when it shouldn't be (for example a barcode like '013655395').

    The fields are returned in either alphabetical order, or if we have the ingestion
    details for the resource at the required version then the order of the fields will
    match the order of the fields in the original source.

    :param resource_id: the resource's id
    :param version: the version of the data we're querying (default: None, which means
        latest)
    :returns: a list of dicts containing the field data
    """
    database = get_database(resource_id)
    data_fields = database.get_data_fields(version)
    parsed_fields = {field.path: field for field in database.get_parsed_fields(version)}

    fields = []
    seen = {'_id'}

    for field in data_fields:
        if field.parsed_path in seen:
            continue
        field_repr = {
            'id': field.parsed_path,
            'type': infer_type(field, parsed_fields),
            'sortable': True,
        }
        if field.children:
            field_repr['sortable'] = False
            if any(child.is_list_element for child in field.children):
                field_repr['type'] = 'array'
        seen.add(field.parsed_path)
        fields.append(field_repr)

    details = get_details_at(resource_id, version)
    if details is None:
        # no details, just order by alphabetical field name
        fields.sort(key=itemgetter('id'))
    else:
        # we have details, order the fields using the order of the columns in the
        # original source
        column_order = details.get_columns(validate=False)

        def key(f: dict) -> int:
            try:
                return column_order.index(f['id'])
            except ValueError:
                return len(column_order)

        fields.sort(key=key)

    # add the _id field to the start of the field list
    fields.insert(0, {'id': '_id', 'type': 'string'})

    return fields
