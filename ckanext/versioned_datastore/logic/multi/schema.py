from ckantools.validators import list_of_strings, list_validator

from ckanext.datastore.logic.schema import json_validator
from ckanext.versioned_datastore.logic.validators import (
    not_empty,
    ignore_missing,
    int_validator,
    boolean_validator,
    are_queryable_resource_ids,
    float_validator,
)

multi_params = {
    "resource_ids": [ignore_missing, are_queryable_resource_ids],
    "query": [ignore_missing, json_validator],
    "version": [ignore_missing, int_validator],
    "query_version": [ignore_missing, str],
}

multi_paging = {
    "after": [ignore_missing, list_validator],
    "size": [ignore_missing, int_validator],
}


def vds_multi_query() -> dict:
    return {**multi_params, **multi_paging}


def vds_multi_count() -> dict:
    return {**multi_params}


def vds_multi_autocomplete_value() -> dict:
    return {
        "field": [not_empty, str],
        "prefix": [ignore_missing, str],
        "case_sensitive": [ignore_missing, boolean_validator],
        "after": [ignore_missing, str],
        "size": [ignore_missing, int_validator],
        **multi_params,
    }


def vds_multi_autocomplete_field() -> dict:
    return {
        "resource_ids": [are_queryable_resource_ids],
        "text": [ignore_missing, str],
        "lowercase": [ignore_missing, boolean_validator],
        "version": [ignore_missing, int_validator],
    }


def vds_multi_hash() -> dict:
    return {
        "query": [json_validator],
        "query_version": [ignore_missing, str],
    }


def vds_multi_fields() -> dict:
    return {
        "resource_ids": [are_queryable_resource_ids],
        "version": [ignore_missing, int_validator],
        "size": [ignore_missing, int_validator],
        "ignore_groups": [ignore_missing, list_of_strings()],
    }


def vds_multi_stats() -> dict:
    return {
        **multi_params,
        "missing": [ignore_missing, float],
        "field": [not_empty, str],
    }