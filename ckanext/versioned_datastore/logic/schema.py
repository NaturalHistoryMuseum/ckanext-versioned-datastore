import json

from ckan.plugins import toolkit
from ckanext.datastore.logic.schema import json_validator, unicode_or_json_validator

# grab all the validator functions upfront
boolean_validator = toolkit.get_validator(u'boolean_validator')
ignore_missing = toolkit.get_validator(u'ignore_missing')
int_validator = toolkit.get_validator(u'int_validator')
not_missing = toolkit.get_validator(u'not_missing')
not_empty = toolkit.get_validator(u'not_empty')
resource_id_exists = toolkit.get_validator(u'resource_id_exists')
OneOf = toolkit.get_validator(u'OneOf')


def list_of_dicts_validator(value, context):
    '''
    Validates that the value passed can be a list of dicts, either because it is or because it is
    once it's been parsed as JSON.

    :param value: the value
    :param context: the context
    :return: the value as a list of dicts
    '''
    # if the value is a string parse it as json first
    if isinstance(value, basestring):
        try:
            value = json.loads(value)
        except ValueError:
            raise toolkit.Invalid(u'Cannot parse JSON')
    # now check that the value is a list and all the elements in the list are dicts
    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
        return value
    # if we reach here the value is rubbish, error out
    raise toolkit.Invalid(u'Value must be a list of dictionaries')


def list_of_strings(delimiter=u','):
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
        if isinstance(value, basestring):
            return value.split(delimiter)
        raise toolkit.Invalid(u'Invalid list of strings')

    return validator


def datastore_search_schema():
    '''
    Returns the schema for the datastore_search action. This is based on the datastore_search from
    the core ckanext-datastore extension, with some parameters removed and others added.

    :return: a dict
    '''
    return {
        u'resource_id': [not_empty, unicode, resource_id_exists],
        u'q': [ignore_missing, unicode_or_json_validator],
        u'filters': [ignore_missing, json_validator],
        u'limit': [ignore_missing, int_validator],
        u'offset': [ignore_missing, int_validator],
        u'fields': [ignore_missing, list_of_strings()],
        u'sort': [ignore_missing, list_of_strings()],
        # add an optional version (if it's left out we default to current)
        u'version': [ignore_missing, int_validator],
        # if a facets list is included then the top 10 most frequent values for each of the fields
        # listed will be returned along with estimated counts
        u'facets': [ignore_missing, list_of_strings()],
        # the facet limits dict allows precise control over how many top values to return for each
        # facet in the facets list
        u'facet_limits': [ignore_missing, json_validator],
        u'run_query': [ignore_missing, boolean_validator],
        u'after': [ignore_missing, json_validator],
    }


def datastore_create_schema():
    return {
        u'resource_id': [ignore_missing, unicode, resource_id_exists],
    }


def datastore_upsert_schema():
    return {
        u'resource_id': [not_empty, unicode, resource_id_exists],
        u'replace': [not_missing, boolean_validator],
        u'version': [ignore_missing, int_validator],
    }


def datastore_delete_schema():
    return {
        u'resource_id': [ignore_missing, unicode, resource_id_exists],
        u'version': [ignore_missing, int_validator],
    }


def datastore_get_record_versions_schema():
    """
    Returns the schema for the datastore_get_record_versions action.

    :return: a dict
    """
    return {
        u'resource_id': [not_empty, unicode, resource_id_exists],
        u'id': [not_empty, int],
    }


def datastore_autocomplete_schema():
    """
    Returns the schema for the datastore_autocomplete action.

    :return: a dict
    """
    return {
        u'resource_id': [not_empty, unicode, resource_id_exists],
        u'q': [ignore_missing, unicode_or_json_validator],
        u'filters': [ignore_missing, json_validator],
        u'limit': [ignore_missing, int_validator],
        u'after': [ignore_missing, unicode],
        u'field': [not_empty, unicode],
        u'term': [not_missing, unicode],
        # add an optional version (if it's left out we default to current)
        u'version': [ignore_missing, int_validator],
    }


def datastore_reindex():
    """
    Returns the schema for the datastore_reindex action.

    :return: a dict
    """
    return {
        u'resource_id': [not_empty, unicode, resource_id_exists],
    }


def datastore_get_rounded_version_schema():
    """
    Returns the schema for the datastore_get_rounded_version action.

    :return: a dict
    """
    return {
        u'resource_id': [not_empty, unicode, resource_id_exists],
        u'version': [ignore_missing, int_validator],
    }


def datastore_search_raw_schema():
    """
    Returns the schema for the datastore_search_raw action.

    :return: a dict
    """
    return {
        u'resource_id': [not_empty, unicode, resource_id_exists],
        u'search': [ignore_missing, json_validator],
        u'version': [ignore_missing, int_validator],
        u'raw_result': [ignore_missing, boolean_validator],
        u'include_version': [ignore_missing, boolean_validator],
    }


def datastore_ensure_privacy_schema():
    '''
    Returns the schema for the datastore_ensure_privacy action.

    :return: a dict
    '''
    return {
        u'resource_id': [ignore_missing, unicode, resource_id_exists],
    }


def datastore_multisearch_schema():
    return {
        u'search': [ignore_missing, json_validator],
        u'version': [ignore_missing, int_validator],
    }


def datastore_field_autocomplete_schema():
    return {
        u'prefix': [ignore_missing, unicode],
        u'resource_ids': [ignore_missing, list_of_strings()],
    }
