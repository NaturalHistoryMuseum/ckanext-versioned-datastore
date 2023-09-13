import re
from ckantools.validators import list_validator, object_validator

from ckan.logic.schema import validator_args
from ckan.plugins import toolkit
from ckan.types import Validator
from ckanext.datastore.logic.schema import json_validator, unicode_or_json_validator
from .arg_objects import QueryArgs, DerivativeArgs, ServerArgs, NotifierArgs


def url_safe(value, context):
    """
    Checks if the value is safe to be included in a URL as a slug.

    :param value: the value to check
    :param context: the context in which to check
    """
    if not re.match("^[A-Za-z0-9-_]+$", value):
        raise toolkit.Invalid(
            "Only a-z, 0-9, hyphens (-) and underscores (_) are valid characters"
        )
    else:
        return value


@validator_args
def datastore_search(
    not_empty: Validator,
    unicode_safe: Validator,
    ignore_missing: Validator,
    int_validator: Validator,
    resource_id_validator: Validator,
    list_of_strings: Validator,
    boolean_validator: Validator,
):
    return {
        "resource_id": [not_empty, unicode_safe, resource_id_validator],
        "q": [ignore_missing, unicode_or_json_validator],
        "filters": [ignore_missing, json_validator],
        "limit": [ignore_missing, int_validator],
        "offset": [ignore_missing, int_validator],
        "fields": [
            ignore_missing,
        ],
        "sort": [ignore_missing, list_of_strings],
        # add an optional version (if it's left out we default to current)
        "version": [ignore_missing, int_validator],
        # if a facets list is included then the top 10 most frequent values for each of
        # the fields listed will be returned along with estimated counts
        "facets": [ignore_missing, list_of_strings],
        # the facet limits dict allows precise control over how many top values to
        # return for each facet in the facets list
        "facet_limits": [ignore_missing, json_validator],
        "run_query": [ignore_missing, boolean_validator],
        "after": [ignore_missing, json_validator],
    }


@validator_args
def datastore_create(
    unicode_safe: Validator, ignore_missing: Validator, resource_id_validator: Validator
):
    return {
        "resource_id": [ignore_missing, unicode_safe, resource_id_validator],
    }


@validator_args
def datastore_upsert(
    not_empty: Validator,
    unicode_safe: Validator,
    ignore_missing: Validator,
    int_validator: Validator,
    resource_id_validator: Validator,
    not_missing: Validator,
    boolean_validator: Validator,
):
    return {
        "resource_id": [not_empty, unicode_safe, resource_id_validator],
        "replace": [not_missing, boolean_validator],
        "version": [ignore_missing, int_validator],
        # 'records': [ignore_missing, list_of_dicts_validator],
    }


@validator_args
def datastore_delete(
    unicode_safe: Validator,
    ignore_missing: Validator,
    int_validator: Validator,
    resource_id_validator: Validator,
):
    return {
        "resource_id": [ignore_missing, unicode_safe, resource_id_validator],
        "version": [ignore_missing, int_validator],
    }


@validator_args
def datastore_get_record_versions(
    unicode_safe: Validator,
    int_validator: Validator,
    not_empty: Validator,
    resource_id_validator: Validator,
):
    return {
        "resource_id": [not_empty, unicode_safe, resource_id_validator],
        "id": [not_empty, int_validator],
    }


@validator_args
def datastore_autocomplete(
    unicode_safe: Validator,
    int_validator: Validator,
    not_empty: Validator,
    resource_id_validator: Validator,
    not_missing: Validator,
    ignore_missing: Validator,
):
    return {
        "resource_id": [not_empty, unicode_safe, resource_id_validator],
        "q": [ignore_missing, unicode_or_json_validator],
        "filters": [ignore_missing, json_validator],
        "limit": [ignore_missing, int_validator],
        "after": [ignore_missing, unicode_safe],
        "field": [not_empty, unicode_safe],
        "term": [not_missing, unicode_safe],
        # add an optional version (if it's left out we default to current)
        "version": [ignore_missing, int_validator],
    }


@validator_args
def datastore_reindex(
    unicode_safe: Validator,
    not_empty: Validator,
    resource_id_validator: Validator,
):
    return {
        "resource_id": [not_empty, unicode_safe, resource_id_validator],
    }


@validator_args
def datastore_get_rounded_version(
    unicode_safe: Validator,
    int_validator: Validator,
    not_empty: Validator,
    resource_id_validator: Validator,
    ignore_missing: Validator,
):
    return {
        "resource_id": [not_empty, unicode_safe, resource_id_validator],
        "version": [ignore_missing, int_validator],
    }


@validator_args
def datastore_search_raw(
    unicode_safe: Validator,
    int_validator: Validator,
    not_empty: Validator,
    resource_id_validator: Validator,
    boolean_validator: Validator,
    ignore_missing: Validator,
):
    return {
        "resource_id": [not_empty, unicode_safe, resource_id_validator],
        "search": [ignore_missing, json_validator],
        "version": [ignore_missing, int_validator],
        "raw_result": [ignore_missing, boolean_validator],
        "include_version": [ignore_missing, boolean_validator],
    }


@validator_args
def datastore_ensure_privacy(
    unicode_safe: Validator,
    resource_id_validator: Validator,
    ignore_missing: Validator,
):
    return {
        "resource_id": [ignore_missing, unicode_safe, resource_id_validator],
    }


@validator_args
def datastore_multisearch(
    unicode_safe: Validator,
    int_validator: Validator,
    boolean_validator: Validator,
    ignore_missing: Validator,
    list_of_strings: Validator,
):
    return {
        "query": [ignore_missing, json_validator],
        "version": [ignore_missing, int_validator],
        "query_version": [ignore_missing, unicode_safe],
        "resource_ids": [ignore_missing, list_of_strings],
        "after": [ignore_missing, list_validator],
        "size": [ignore_missing, int_validator],
        "top_resources": [ignore_missing, boolean_validator],
        "resource_ids_and_versions": [ignore_missing, json_validator],
        "timings": [ignore_missing, boolean_validator],
    }


@validator_args
def datastore_field_autocomplete(
    unicode_safe: Validator,
    boolean_validator: Validator,
    ignore_missing: Validator,
    list_of_strings: Validator,
):
    return {
        "text": [ignore_missing, unicode_safe],
        "resource_ids": [ignore_missing, list_of_strings],
        "lowercase": [ignore_missing, boolean_validator],
    }


@validator_args
def datastore_value_autocomplete(
    unicode_safe: Validator,
    int_validator: Validator,
    ignore_missing: Validator,
    list_of_strings: Validator,
    not_empty: Validator,
):
    return {
        "field": [not_empty, unicode_safe],
        "prefix": [ignore_missing, unicode_safe],
        "query": [ignore_missing, json_validator],
        "version": [ignore_missing, int_validator],
        "query_version": [ignore_missing, unicode_safe],
        "resource_ids": [ignore_missing, list_of_strings],
        "after": [ignore_missing, unicode_safe],
        "size": [ignore_missing, int_validator],
        "resource_ids_and_versions": [ignore_missing, json_validator],
    }


@validator_args
def datastore_create_slug(
    unicode_safe: Validator,
    int_validator: Validator,
    ignore_missing: Validator,
    list_of_strings: Validator,
    boolean_validator: Validator,
):
    return {
        "query": [ignore_missing, json_validator],
        "version": [ignore_missing, int_validator],
        "query_version": [ignore_missing, unicode_safe],
        "resource_ids": [ignore_missing, list_of_strings],
        "resource_ids_and_versions": [ignore_missing, json_validator],
        "pretty_slug": [ignore_missing, boolean_validator],
        "nav_slug": [ignore_missing, boolean_validator],
    }


@validator_args
def datastore_resolve_slug(unicode_safe: Validator):
    return {
        "slug": [unicode_safe],
    }


@validator_args
def datastore_count(
    int_validator: Validator,
    ignore_missing: Validator,
    list_of_strings: Validator,
):
    return {
        "resource_ids": [ignore_missing, list_of_strings],
        "version": [ignore_missing, int_validator],
    }


@validator_args
def datastore_queue_download(
    ignore_missing: Validator,
    not_missing: Validator,
):
    return {
        "query": [not_missing, object_validator(QueryArgs)],
        # called file instead of derivative to make its purpose clearer to the end user
        "file": [
            not_missing,
            object_validator(DerivativeArgs),
        ],
        "server": [ignore_missing, object_validator(ServerArgs)],
        "notifier": [ignore_missing, object_validator(NotifierArgs)],
    }


@validator_args
def datastore_regenerate_download(
    not_missing: Validator,
    ignore_missing: Validator,
    unicode_safe: Validator,
):
    return {
        "download_id": [not_missing, unicode_safe],
        "server": [ignore_missing, object_validator(ServerArgs)],
        "notifier": [ignore_missing, object_validator(NotifierArgs)],
    }


@validator_args
def datastore_guess_fields(
    list_of_strings: Validator,
    ignore_missing: Validator,
    unicode_safe: Validator,
    int_validator: Validator,
):
    return {
        "query": [ignore_missing, json_validator],
        "query_version": [ignore_missing, unicode_safe],
        "version": [ignore_missing, int_validator],
        "resource_ids": [ignore_missing, list_of_strings],
        "resource_ids_and_versions": [ignore_missing, json_validator],
        "size": [ignore_missing, int_validator],
        "ignore_groups": [ignore_missing, list_of_strings],
    }


@validator_args
def datastore_hash_query(ignore_missing: Validator, unicode_safe: Validator):
    return {
        "query": [ignore_missing, json_validator],
        "query_version": [ignore_missing, unicode_safe],
    }


@validator_args
def datastore_is_datastore_resource(
    not_missing: Validator, not_empty: Validator, resource_id_validator: Validator
):
    return {"resource_id": [not_missing, not_empty, resource_id_validator]}


@validator_args
def datastore_edit_slug(
    not_missing: Validator, not_empty: Validator, unicode_safe: Validator
):
    return {
        "current_slug": [not_missing, not_empty, unicode_safe],
        "new_reserved_slug": [not_missing, not_empty, unicode_safe, url_safe],
    }
