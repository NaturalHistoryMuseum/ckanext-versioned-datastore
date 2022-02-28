import json

from ckan.plugins import toolkit
from ckanext.datastore.logic.schema import json_validator, unicode_or_json_validator

# grab all the validator functions upfront
boolean_validator = toolkit.get_validator('boolean_validator')
ignore_missing = toolkit.get_validator('ignore_missing')
int_validator = toolkit.get_validator('int_validator')
not_missing = toolkit.get_validator('not_missing')
not_empty = toolkit.get_validator('not_empty')
resource_id_exists = toolkit.get_validator('resource_id_exists')
email_validator = toolkit.get_validator('email_validator')


def list_of_dicts_validator(value, context):
    '''
    Validates that the value passed can be a list of dicts, either because it is or because it is
    once it's been parsed as JSON.

    :param value: the value
    :param context: the context
    :return: the value as a list of dicts
    '''
    # if the value is a string parse it as json first
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            raise toolkit.Invalid('Cannot parse JSON')
    # now check that the value is a list and all the elements in the list are dicts
    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
        return value
    # if we reach here the value is rubbish, error out
    raise toolkit.Invalid('Value must be a list of dictionaries')


def list_of_strings(delimiter=','):
    '''
    Creates a converter/validator function which when given a value return a list or raises an error
    if a list can't be created from the value. If the value passed in is a list already it is
    returned with no modifications, if it's a string then the delimiter is used to split the string
    and the result is returned. If the value is neither a list or a string then an error is raised.

    :param delimiter: the string to delimit the value on, if it's a string. Defaults to a comma
    :return: a list
    '''

    def validator(value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return value.split(delimiter)
        raise toolkit.Invalid('Invalid list of strings')

    return validator


def list_validator(value, context):
    '''
    Checks that the given value is a list. If it is then it is allowed to pass, if not an Invalid
    error is raised. If the value is a string then we attempt to parse it as a JSON serialised list
    and raise an exception if we can't.

    :param value: the value to check
    :param context: the context in which to check
    :return:
    '''
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            raise toolkit.Invalid('Cannot parse JSON list')
    if isinstance(value, list):
        return value
    else:
        raise toolkit.Invalid('Value must be a list')


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


def datastore_create_slug():
    return {
        'query': [ignore_missing, json_validator],
        'version': [ignore_missing, int_validator],
        'query_version': [ignore_missing, str],
        'resource_ids': [ignore_missing, list_of_strings()],
        'resource_ids_and_versions': [ignore_missing, json_validator],
        'pretty_slug': [ignore_missing, boolean_validator],
    }


def datastore_resolve_slug():
    return {
        'slug': [str],
    }


def datastore_count():
    return {
        'resource_ids': [ignore_missing, list_of_strings()],
        'version': [ignore_missing, int_validator]
    }

def datastore_queue_download():
    return {
        'email_address': [not_missing, not_empty, email_validator],
        'query': [ignore_missing, json_validator],
        'version': [ignore_missing, int_validator],
        'resource_ids_and_versions': [ignore_missing, json_validator],
        'query_version': [ignore_missing, str],
        'resource_ids': [ignore_missing, list_of_strings()],
        'separate_files': [ignore_missing, boolean_validator],
        'format': [ignore_missing, str],
        'ignore_empty_fields': [ignore_missing, boolean_validator],
        'format_args': [ignore_missing, json_validator]
    }


def datastore_guess_fields():
    return {
        'query': [ignore_missing, json_validator],
        'query_version': [ignore_missing, str],
        'version': [ignore_missing, int_validator],
        'resource_ids': [ignore_missing, list_of_strings()],
        'resource_ids_and_versions': [ignore_missing, json_validator],
        'size': [ignore_missing, int_validator],
        'ignore_groups': [ignore_missing, list_of_strings()]
    }


def datastore_hash_query():
    return {
        'query': [ignore_missing, json_validator],
        'query_version': [ignore_missing, str],
    }


def datastore_is_datastore_resource():
    return {
        'resource_id': [not_missing, not_empty, resource_id_exists]
    }
