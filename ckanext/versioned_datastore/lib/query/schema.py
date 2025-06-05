import abc
import itertools
import json
from collections import OrderedDict
from typing import List

from elasticsearch_dsl.query import Query as DSLQuery
from importlib_resources import files
from jsonschema.validators import RefResolver, validator_for

schemas = OrderedDict()
schema_base_path = (
    files('ckanext.versioned_datastore.theme') / 'public' / 'querySchemas'
)


def register_schema(version: str, schema: dict):
    """
    Registers a new schema with the given version into the central schemas dict. The
    schema parameter should be a subclass of the Schema class but generally must at
    least provide the translate and validate methods. After registration, the schemas
    dict is updated to ensure the correct sort order is in use.

    :param version: the query schema version
    :param schema: the Schema object representing the query schema
    """
    global schemas
    # add the new version and re-sort the schemas by version in ascending order
    # (h/t https://stackoverflow.com/a/2574090)
    schemas = OrderedDict(
        sorted(
            itertools.chain([(version, schema)], schemas.items()),
            key=lambda vs: [int(u) for u in vs[0][1:].split('.')],
        )
    )


def get_latest_query_version() -> str:
    """
    Gets the latest query version from the registered schemas dict.

    :returns: the latest query version
    """
    return next(iter(schemas.keys()))


class InvalidQuerySchemaVersionError(Exception):
    def __init__(self, version):
        super(Exception, self).__init__(f'Invalid query version: {version}')


def validate_query(query: dict, version: str) -> bool:
    """
    Validate the given query dict against the query schema for the given version. If the
    version doesn't match any registered schemas then an InvalidQuerySchemaVersionError
    will be raised.

    :param query: the query dict
    :param version: the query schema version to validate against
    :returns: True if the validation succeeded, otherwise jsonschema exceptions will be
        raised
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    get_schema(version).validate(query)
    return True


def translate_query(query: dict, version: str) -> DSLQuery:
    """
    Translates the given query dict into an elasticsearch-dsl object using the Schema
    object associated with the given version. If the version doesn't match any
    registered schemas then an InvalidQuerySchemaVersionError will be raised.

    :param query: the whole query dict
    :param version: the query schema version to translate using
    :returns: an instantiated Elasticsearch DSL Query object
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    else:
        return get_schema(version).translate(query)


def hash_query(query: dict, version: str) -> str:
    """
    Hashes the given query at the given version and returns the unique digest.

    :param query: the query dict
    :param version: the query version
    :returns: the hash
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    else:
        return get_schema(version).hash(query)


def get_schema(version: str) -> 'Schema':
    return schemas[version]


def get_schema_versions() -> List[str]:
    return list(schemas.keys())


def normalise_query(query, version):
    """
    Corrects some (small) common query errors, e.g. removing empty groups.

    :param query: the query dict
    :param version: the query version
    :returns: the corrected/normalised query
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    else:
        return schemas[version].normalise(query)


def load_core_schema(version):
    """
    Given a query schema version, loads the schema from the schema_base_path directory.

    :param version: the version to load
    :returns: the loaded schema (as a dict) and a jsonschmea validator object for the
        schema
    """
    schema_file = schema_base_path / version / f'{version}.json'
    schema = json.loads(schema_file.read_text('utf-8'))
    validator_cls = validator_for(schema)
    validator_cls.check_schema(schema)
    # create a resolver which can resolve refs relative to the schema
    resolver = RefResolver(base_uri=f'file://{schema_file}', referrer=schema)
    validator = validator_cls(schema, resolver=resolver)
    return schema, validator


class Schema(abc.ABC):
    """
    Abstract base class for a query schema.

    Should be extended by all registered query schemas.
    """

    @abc.abstractmethod
    def validate(self, query: dict):
        """
        Validate the given query against this schema. Failures are marked raising
        jsonschema exceptions.

        :param query: the query dict to validate
        """
        pass

    @abc.abstractmethod
    def translate(self, query: dict) -> DSLQuery:
        """
        Translates the query into an Elasticsearch DSL object.

        :param query: the whole query dict
        :returns: an instantiated Elasticsearch DSL object
        """
        pass

    @abc.abstractmethod
    def hash(self, query: dict) -> str:
        """
        Hashes the query and returns the hex digest.

        :param query: the whole query dict
        :returns: a string hex digest
        """
        pass

    def normalise(self, query: dict) -> dict:
        """
        Corrects some (small) common query errors, e.g. removing empty groups.

        :param query: the query dict
        :returns: the corrected/normalised query dict
        """
        return query
