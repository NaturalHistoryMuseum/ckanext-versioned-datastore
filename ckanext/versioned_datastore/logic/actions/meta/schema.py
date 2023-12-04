import json
import re

from ckan.plugins import toolkit
from ckanext.datastore.logic.schema import json_validator, unicode_or_json_validator
from ckantools.validators import list_validator, list_of_strings, object_validator
from .arg_objects import QueryArgs, DerivativeArgs, ServerArgs, NotifierArgs

# grab all the validator functions upfront
boolean_validator = toolkit.get_validator('boolean_validator')
ignore_missing = toolkit.get_validator('ignore_missing')
int_validator = toolkit.get_validator('int_validator')
not_missing = toolkit.get_validator('not_missing')
not_empty = toolkit.get_validator('not_empty')
resource_id_exists = toolkit.get_validator('resource_id_exists')
email_validator = toolkit.get_validator('email_validator')


def url_safe(value, context):
    """
    Checks if the value is safe to be included in a URL as a slug.

    :param value: the value to check
    :param context: the context in which to check
    """
    if not re.match('^[A-Za-z0-9-_]+$', value):
        raise toolkit.Invalid(
            'Only a-z, 0-9, hyphens (-) and underscores (_) are valid characters'
        )
    else:
        return value


def datastore_search():
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'q': [ignore_missing, unicode_or_json_validator],
        'filters': [ignore_missing, json_validator],
        'limit': [ignore_missing, int_validator],
        'offset': [ignore_missing, int_validator],
        'fields': [ignore_missing, list_of_strings()],
        'sort': [ignore_missing, list_of_strings()],
        # add an optional version (if it's left out we default to current)
        'version': [ignore_missing, int_validator],
        # if a facets list is included then the top 10 most frequent values for each of the fields
        # listed will be returned along with estimated counts
        'facets': [ignore_missing, list_of_strings()],
        # the facet limits dict allows precise control over how many top values to return for each
        # facet in the facets list
        'facet_limits': [ignore_missing, json_validator],
        'run_query': [ignore_missing, boolean_validator],
        'after': [ignore_missing, json_validator],
    }


def datastore_create():
    return {
        'resource_id': [ignore_missing, str, resource_id_exists],
    }


def datastore_upsert():
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'replace': [not_missing, boolean_validator],
        'version': [ignore_missing, int_validator],
        # 'records': [ignore_missing, list_of_dicts_validator],
    }


def datastore_delete():
    return {
        'resource_id': [ignore_missing, str, resource_id_exists],
        'version': [ignore_missing, int_validator],
    }


def datastore_get_record_versions():
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'id': [not_empty, int],
    }


def datastore_autocomplete():
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'q': [ignore_missing, unicode_or_json_validator],
        'filters': [ignore_missing, json_validator],
        'limit': [ignore_missing, int_validator],
        'after': [ignore_missing, str],
        'field': [not_empty, str],
        'term': [not_missing, str],
        # add an optional version (if it's left out we default to current)
        'version': [ignore_missing, int_validator],
    }


def datastore_reindex():
    return {
        'resource_id': [not_empty, str, resource_id_exists],
    }


def datastore_get_rounded_version():
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'version': [ignore_missing, int_validator],
    }


def datastore_search_raw():
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'search': [ignore_missing, json_validator],
        'version': [ignore_missing, int_validator],
        'raw_result': [ignore_missing, boolean_validator],
        'include_version': [ignore_missing, boolean_validator],
    }


def datastore_ensure_privacy():
    return {
        'resource_id': [ignore_missing, str, resource_id_exists],
    }


def datastore_multisearch():
    return {
        'query': [ignore_missing, json_validator],
        'version': [ignore_missing, int_validator],
        'query_version': [ignore_missing, str],
        'resource_ids': [ignore_missing, list_of_strings()],
        'after': [ignore_missing, list_validator],
        'size': [ignore_missing, int_validator],
        'top_resources': [ignore_missing, boolean_validator],
        'resource_ids_and_versions': [ignore_missing, json_validator],
        'timings': [ignore_missing, boolean_validator],
    }


def datastore_field_autocomplete():
    return {
        'text': [ignore_missing, str],
        'resource_ids': [ignore_missing, list_of_strings()],
        'lowercase': [ignore_missing, boolean_validator],
    }


def datastore_value_autocomplete():
    return {
        'field': [not_empty, str],
        'prefix': [ignore_missing, str],
        'query': [ignore_missing, json_validator],
        'version': [ignore_missing, int_validator],
        'query_version': [ignore_missing, str],
        'resource_ids': [ignore_missing, list_of_strings()],
        'after': [ignore_missing, str],
        'size': [ignore_missing, int_validator],
        'resource_ids_and_versions': [ignore_missing, json_validator],
    }


def datastore_create_slug():
    return {
        'query': [ignore_missing, json_validator],
        'version': [ignore_missing, int_validator],
        'query_version': [ignore_missing, str],
        'resource_ids': [ignore_missing, list_of_strings()],
        'resource_ids_and_versions': [ignore_missing, json_validator],
        'pretty_slug': [ignore_missing, boolean_validator],
        'nav_slug': [ignore_missing, boolean_validator],
    }


def datastore_resolve_slug():
    return {
        'slug': [str],
    }


def datastore_count():
    return {
        'resource_ids': [ignore_missing, list_of_strings()],
        'version': [ignore_missing, int_validator],
    }


def datastore_queue_download():
    return {
        'query': [not_missing, object_validator(QueryArgs)],
        'file': [
            not_missing,
            object_validator(DerivativeArgs),
        ],  # called 'file' instead of derivative to make its purpose clearer to the end user
        'server': [ignore_missing, object_validator(ServerArgs)],
        'notifier': [ignore_missing, object_validator(NotifierArgs)],
    }


def datastore_regenerate_download():
    return {
        'download_id': [not_missing, str],
        'server': [ignore_missing, object_validator(ServerArgs)],
        'notifier': [ignore_missing, object_validator(NotifierArgs)],
    }


def datastore_guess_fields():
    return {
        'query': [ignore_missing, json_validator],
        'query_version': [ignore_missing, str],
        'version': [ignore_missing, int_validator],
        'resource_ids': [ignore_missing, list_of_strings()],
        'resource_ids_and_versions': [ignore_missing, json_validator],
        'size': [ignore_missing, int_validator],
        'ignore_groups': [ignore_missing, list_of_strings()],
    }


def datastore_hash_query():
    return {
        'query': [ignore_missing, json_validator],
        'query_version': [ignore_missing, str],
    }


def datastore_is_datastore_resource():
    return {'resource_id': [not_missing, not_empty, resource_id_exists]}


def datastore_edit_slug():
    return {
        'current_slug': [str, not_missing, not_empty],
        'new_reserved_slug': [str, not_missing, not_empty, url_safe],
    }


def datastore_multisearch_counts():
    return {
        'query': [ignore_missing, json_validator],
        'version': [ignore_missing, int_validator],
        'query_version': [ignore_missing, str],
        'resource_ids': [ignore_missing, list_of_strings()],
        'resource_ids_and_versions': [ignore_missing, json_validator],
    }
