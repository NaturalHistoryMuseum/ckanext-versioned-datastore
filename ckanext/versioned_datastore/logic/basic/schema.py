from ckantools.validators import list_of_strings, list_validator

from ckanext.datastore.logic.schema import json_validator, unicode_or_json_validator
from ckanext.versioned_datastore.logic.validators import (
    boolean_validator,
    ignore_missing,
    int_validator,
    not_empty,
    not_missing,
    validate_datastore_resource_id,
)

# common parameters used for searching a resource with a query and a version
basic_params = {
    'resource_id': [not_empty, str, validate_datastore_resource_id],
    'q': [ignore_missing, unicode_or_json_validator],
    'filters': [ignore_missing, json_validator],
    'version': [ignore_missing, int_validator],
}
# common parameters for paging
basic_paging = {
    'limit': [ignore_missing, int_validator],
    'offset': [ignore_missing, int_validator],
    'after': [ignore_missing, list_validator],
}


def vds_basic_query() -> dict:
    return {
        **basic_params,
        **basic_paging,
        'fields': [ignore_missing, list_of_strings()],
        'sort': [ignore_missing, list_of_strings()],
        'facets': [ignore_missing, list_of_strings()],
        'facet_limits': [ignore_missing, json_validator],
        'run_query': [ignore_missing, boolean_validator],
    }


def vds_basic_count() -> dict:
    return {**basic_params}


def vds_basic_autocomplete() -> dict:
    return {
        'field': [not_empty, str],
        'term': [not_missing, str],
        **basic_params,
        **basic_paging,
    }


def vds_basic_extent() -> dict:
    return {**basic_params}
