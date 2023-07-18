from ckantools.validators import list_of_strings
from ckantools.validators.ivalidators import BaseArgs

from ckan.plugins import toolkit
from ckanext.datastore.logic.schema import json_validator

# grab all the validator functions upfront
boolean_validator = toolkit.get_validator('boolean_validator')
ignore_missing = toolkit.get_validator('ignore_missing')
int_validator = toolkit.get_validator('int_validator')
not_missing = toolkit.get_validator('not_missing')
not_empty = toolkit.get_validator('not_empty')
resource_id_exists = toolkit.get_validator('resource_id_exists')
email_validator = toolkit.get_validator('email_validator')


class QueryArgs(BaseArgs):
    query: dict
    query_version: str
    version: int
    resource_ids: list
    resource_ids_and_versions: dict
    slug_or_doi: str

    fields = {
        'query': [ignore_missing, json_validator],
        'query_version': [ignore_missing, str],
        'version': [ignore_missing, int_validator],
        'resource_ids': [ignore_missing, list_of_strings()],
        'resource_ids_and_versions': [ignore_missing, json_validator],
        'slug_or_doi': [ignore_missing, str],
    }


class DerivativeArgs(BaseArgs):
    format: str
    format_args: dict
    separate_files: bool
    ignore_empty_fields: bool
    transform: dict

    fields = {
        'format': [not_missing, str],
        'format_args': [ignore_missing, json_validator],
        'separate_files': [ignore_missing, boolean_validator],
        'ignore_empty_fields': [ignore_missing, boolean_validator],
        'transform': [ignore_missing, json_validator],
    }

    defaults = {
        'format_args': {},
        'separate_files': False,
        'ignore_empty_fields': False,
        'transform': {},
    }


class ServerArgs(BaseArgs):
    type: str
    type_args: dict
    custom_filename: str

    fields = {
        'type': [ignore_missing, str],
        'type_args': [ignore_missing, json_validator],
        'custom_filename': [ignore_missing, str],
    }

    defaults = {'type': 'direct', 'type_args': {}}


class NotifierArgs(BaseArgs):
    type: str
    type_args: dict

    fields = {
        'type': [ignore_missing, str],
        'type_args': [ignore_missing, json_validator],
    }

    defaults = {'type': 'none', 'type_args': {}}
