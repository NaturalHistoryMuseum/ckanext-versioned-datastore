from datetime import datetime

from ckan.plugins import toolkit
from splitgill.utils import to_timestamp

from ..query.schema import (
    get_latest_query_version,
    hash_query,
    translate_query,
    validate_query,
)
from ..query.utils import get_available_datastore_resources, get_resources_and_versions
from .. import common
from ..datastore_utils import prefix_resource
from ...logic.actions.meta.arg_objects import QueryArgs
import hashlib
from ..basic_query.utils import convert_to_multisearch
from jsonschema.exceptions import ValidationError


class Query(object):
    def __init__(self, query, query_version, resource_ids_and_versions):
        self.query = query
        self.query_version = query_version
        self.resource_ids_and_versions = resource_ids_and_versions
        self._search = None
        self.validate()

    @property
    def search(self):
        if self._search is None:
            self._search = self.translate()
        return self._search

    @classmethod
    def from_query_args(cls, query_args: QueryArgs):
        query = query_args.query
        query_version = query_args.query_version
        resource_ids = query_args.resource_ids
        resource_ids_and_versions = query_args.resource_ids_and_versions
        version = query_args.version

        if query_args.slug_or_doi:
            try:
                saved_query = toolkit.get_action('datastore_resolve_slug')(
                    {}, {'slug': query_args.slug_or_doi}
                )
                query = saved_query.get('query')
                query_version = saved_query.get('query_version')
                resource_ids = saved_query.get('resource_ids')
                resource_ids_and_versions = saved_query.get('resource_ids_and_versions')
            except toolkit.ValidationError:
                # if the slug doesn't resolve, continue as normal
                pass

        resource_ids, rounded_resource_ids_and_versions = get_resources_and_versions(
            resource_ids, resource_ids_and_versions
        )

        # setup the query
        if query is None:
            query = {}
        if query_version is None:
            query_version = get_latest_query_version()
            try:
                validate_query(query, query_version)
            except ValidationError:
                query = convert_to_multisearch(query)

        return cls(query, query_version, rounded_resource_ids_and_versions)

    @property
    def hash(self):
        return hash_query(self.query, self.query_version)

    @property
    def resource_hash(self):
        resources = sorted(self.resource_ids_and_versions.items())
        return hashlib.sha1('|'.join(map(str, resources)).encode('utf-8')).hexdigest()

    @property
    def record_hash(self):
        to_hash = [self.hash, self.resource_hash]
        download_hash = hashlib.sha1('|'.join(to_hash).encode('utf-8'))
        return download_hash.hexdigest()

    def validate(self):
        return validate_query(self.query, self.query_version)

    def translate(self):
        return translate_query(self.query, self.query_version)
