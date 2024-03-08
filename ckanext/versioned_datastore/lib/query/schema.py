import io
import json
from collections import OrderedDict

import abc
import itertools
import os
import six
from jsonschema.validators import validator_for, RefResolver
from importlib_resources import files

schemas = OrderedDict()
schema_base_path = files('ckanext.versioned_datastore.theme').joinpath(
    'public/querySchemas'
)


def register_schema(version, schema):
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


def get_latest_query_version():
    """
    Gets the latest query version from the registered schemas dict.

    :return: the latest query version
    """
    return next(iter(schemas.keys()))


class InvalidQuerySchemaVersionError(Exception):
    def __init__(self, version):
        super(Exception, self).__init__(f'Invalid query version: {version}')


def validate_query(query, version):
    """
    Validate the given query dict against the query schema for the given version. If the
    version doesn't match any registered schemas then an InvalidQuerySchemaVersionError
    will be raised.

    :param query: the query dict
    :param version: the query schema version to validate against
    :return: True if the validation succeeded, otherwise jsonschema exceptions will be raised
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    schemas[version].validate(query)
    return True


def translate_query(query, version, search=None):
    """
    Translates the given query dict into an elasticsearch-dsl object using the Schema
    object associated with the given version. If the version doesn't match any
    registered schemas then an InvalidQuerySchemaVersionError will be raised.

    :param query: the whole query dict
    :param version: the query schema version to translate using
    :param search: an instantiated elasticsearch-dsl object to be built on instead of creating
                   a fresh object. By default a new search object is created.
    :return: an instantiated elasticsearch-dsl object
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    else:
        return schemas[version].translate(query, search=search)


def hash_query(query, version):
    """
    Hashes the given query at the given version and returns the unique digest.

    :param query: the query dict
    :param version: the query version
    :return: the hash
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    else:
        return schemas[version].hash(query)


def normalise_query(query, version):
    """
    Corrects some (small) common query errors, e.g. removing empty groups.

    :param query: the query dict
    :param version: the query version
    :return: the corrected/normalised query
    """
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    else:
        return schemas[version].normalise(query)


def load_core_schema(version):
    """
    Given a query schema version, loads the schema from the schema_base_path directory.

    :param version: the version to load
    :return: the loaded schema (as a dict) and a jsonschmea validator object for the schema
    """
    schema_file = schema_base_path.joinpath(version).joinpath(f'{version}.json')
    with io.open(schema_file, 'r', encoding='utf-8') as f:
        schema = json.load(f)
        validator_cls = validator_for(schema)
        validator_cls.check_schema(schema)
        # create a resolver which can resolve refs relative to the schema
        resolver = RefResolver(base_uri=f'file://{schema_file}', referrer=schema)
        validator = validator_cls(schema, resolver=resolver)
        return schema, validator


@six.add_metaclass(abc.ABCMeta)
class Schema(object):
    """
    Abstract base class for a query schema.

    Should be extended by all registered query schemas.
    """

    @abc.abstractmethod
    def validate(self, query):
        """
        Validate the given query against this schema. Failures are marked raising
        jsonschema exceptions.

        :param query: the query dict to validate
        """
        pass

    @abc.abstractmethod
    def translate(self, query, search=None):
        """
        Translates the query into an elasticsearch-dsl search object.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object to be built on instead of creating
                       a fresh object. By default a new search object is created.
        :return: an instantiated elasticsearch-dsl object
        """
        pass

    @abc.abstractmethod
    def hash(self, query):
        """
        Hashes the query and returns the hex digest.

        :param query: the whole query dict
        :return: a string hex digest
        """
        pass

    def normalise(self, query):
        """
        Corrects some (small) common query errors, e.g. removing empty groups.

        :param query: the query dict
        :return: the corrected/normalised query dict
        """
        return query
