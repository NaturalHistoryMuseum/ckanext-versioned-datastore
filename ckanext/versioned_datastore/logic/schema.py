from ckan.logic import get_validator
from ckan.logic.validators import Invalid
from ckanext.datastore.logic.schema import json_validator, unicode_or_json_validator

# grab all the validator functions upfront
boolean_validator = get_validator('boolean_validator')
empty = get_validator('empty')
ignore_missing = get_validator('ignore_missing')
int_validator = get_validator('int_validator')
not_empty = get_validator('not_empty')
not_missing = get_validator('not_missing')


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
        raise Invalid(u'Invalid list of strings')
    return validator


def versioned_datastore_search_schema():
    '''
    Returns the schema for the datastore_search action. This is based on the datastore_search from
    the core ckanext-datastore extension, with some parameters removed and others added.

    :return: a dict
    '''
    return {
        u'resource_id': [not_missing, not_empty, unicode],
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
        u'__junk': [empty],
    }
