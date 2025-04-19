from ckan.plugins import toolkit
from ckantools.validators.ivalidators import BaseArgs

from ckanext.datastore.logic.schema import json_validator
from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.query.utils import convert_to_multisearch
from ckanext.versioned_datastore.lib.utils import get_available_datastore_resources
from ckanext.versioned_datastore.logic.validators import (
    boolean_validator,
    ignore_missing,
    int_validator,
    not_missing,
    validate_datastore_resource_ids,
    validate_resource_ids,
)


class QueryArgs(BaseArgs):
    resource_ids: list
    query: dict
    query_version: str
    version: int
    slug_or_doi: str

    fields = {
        'resource_ids': [ignore_missing, validate_resource_ids],
        'query': [ignore_missing, json_validator],
        'query_version': [ignore_missing, str],
        'version': [ignore_missing, int_validator],
        'slug_or_doi': [ignore_missing, str],
    }

    def validate(self):
        if self.query and 'resource_ids' in self.query:
            validate_datastore_resource_ids(self.query['resource_ids'])
            if not self.resource_ids:
                self.resource_ids = self.query['resource_ids']

    def to_schema_query(self) -> SchemaQuery:
        query = self.query
        query_version = self.query_version
        resource_ids = self.resource_ids
        version = self.version

        if self.slug_or_doi:
            try:
                saved_query = toolkit.get_action('vds_slug_resolve')(
                    {}, {'slug': self.slug_or_doi}
                )
                query = saved_query.get('query')
                query_version = saved_query.get('query_version')
                resource_ids = saved_query.get('resource_ids')
                version = saved_query.get('version')
            except toolkit.ValidationError:
                # if the slug doesn't resolve, continue as normal
                pass

        if query_version and query_version.lower().startswith('v0'):
            # this is an old/basic query, so we need to convert it first
            query = convert_to_multisearch(query)
            query_version = None

        # if no resource IDs have been provided, use all resources available to the user
        if not resource_ids:
            resource_ids = list(get_available_datastore_resources())

        # schema query init will handle defaulting the various parts
        return SchemaQuery(resource_ids, version, query, query_version)


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

    def ensure_defaults_are_set(self):
        for field, default_value in self.defaults.items():
            if getattr(self, field) is None:
                setattr(self, field, default_value)


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
