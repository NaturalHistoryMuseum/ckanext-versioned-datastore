import hashlib
import io
import json
import random
from collections import OrderedDict

import abc
import dicthash
import itertools
import os
import six
from ckan import model
from ckan.plugins import toolkit
from jsonschema.validators import validator_for, RefResolver
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

from .slug_words import adjectives, animals
from .. import utils
from ...model.slugs import DatastoreSlug

schemas = OrderedDict()
schema_base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), u'..', u'..', u'theme',
                                                u'public', u'querySchemas'))
query_key_template = u'query:{}'
slug_key_template = u'slug:{}'


def register_schema(version, schema):
    '''
    Registers a new schema with the given version into the central schemas dict. The schema
    parameter should be a subclass of the Schema class but generally must at least provide the
    translate and validate methods. After registration, the schemas dict is updated to ensure the
    correct sort order is in use.

    :param version: the query schema version
    :param schema: the Schema object representing the query schema
    '''
    global schemas
    # add the new version and re-sort the schemas by version in ascending order
    # (h/t https://stackoverflow.com/a/2574090)
    schemas = OrderedDict(sorted(itertools.chain([(version, schema)], schemas.items()),
                                 key=lambda vs: [int(u) for u in vs[0][1:].split(u'.')]))


def get_latest_query_version():
    '''
    Gets the latest query version from the registered schemas dict.

    :return: the latest query version
    '''
    return next(iter(schemas.keys()))


class InvalidQuerySchemaVersionError(Exception):

    def __init__(self, version):
        super(Exception, self).__init__(u'Invalid query version: {}'.format(version))


def validate_query(query, version):
    '''
    Validate the given query dict against the query schema for the given version. If the version
    doesn't match any registered schemas then an InvalidQuerySchemaVersionError will be raised.

    :param query: the query dict
    :param version: the query schema version to validate against
    :return: True if the validation succeeded, otherwise jsonschema exceptions will be raised
    '''
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    schemas[version].validate(query)
    return True


def translate_query(query, version, search=None):
    '''
    Translates the given query dict into an elasticsearch-dsl object using the Schema object
    associated with the given version. If the version doesn't match any registered schemas then an
    InvalidQuerySchemaVersionError will be raised.

    :param query: the whole query dict
    :param version: the query schema version to translate using
    :param search: an instantiated elasticsearch-dsl object to be built on instead of creating
                   a fresh object. By default a new search object is created.
    :return: an instantiated elasticsearch-dsl object
    '''
    if version not in schemas:
        raise InvalidQuerySchemaVersionError(version)
    else:
        return schemas[version].translate(query, search=search)


def load_core_schema(version):
    '''
    Given a query schema version, loads the schema from the schema_base_path directory.

    :param version: the version to load
    :return: the loaded schema (as a dict) and a jsonschmea validator object for the schema
    '''
    schema_file = os.path.join(schema_base_path, version, u'{}.json'.format(version))
    with io.open(schema_file, u'r', encoding=u'utf-8') as f:
        schema = json.load(f)
        validator_cls = validator_for(schema)
        validator_cls.check_schema(schema)
        # create a resolver which can resolve refs relative to the schema
        resolver = RefResolver(base_uri='file://{}'.format(schema_file), referrer=schema)
        validator = validator_cls(schema, resolver=resolver)
        return schema, validator


@six.add_metaclass(abc.ABCMeta)
class Schema(object):
    '''
    Abstract base class for a query schema. Should be extended by all registered query schemas.
    '''

    @abc.abstractmethod
    def validate(self, query):
        '''
        Validate the given query against this schema. Failures are marked raising jsonschema
        exceptions.

        :param query: the query dict to validate
        '''
        pass

    @abc.abstractmethod
    def translate(self, query, search=None):
        '''
        Translates the query into an elasticsearch-dsl search object.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object to be built on instead of creating
                       a fresh object. By default a new search object is created.
        :return: an instantiated elasticsearch-dsl object
        '''
        pass


def generate_query_hash(query, query_version, version, resource_ids, resource_ids_and_versions):
    '''
    Given a query and the parameters required to run it, create a unique id for it (a hash) and
    returns it. The hashing algorithm used is sha1 and the output will be a 40 character hexidecimal
    string.

    :param query: the query dict
    :param query_version: the query version
    :param version: the data version
    :param resource_ids: the ids of the resources under search
    :param resource_ids_and_versions: the resource ids and specific versions to search at for them
    :return: a unique id for the query, which is a hash of the query and parameters
    '''
    to_hash = {
        u'query': query,
        u'query_version': query_version,
        u'version': version,
        # sort the resources to ensure the hash doesn't change unnecessarily
        u'resource_ids': sorted(resource_ids) if resource_ids is not None else resource_ids,
        u'resource_ids_and_versions': resource_ids_and_versions,
    }
    raw = dicthash.generate_hash_from_dict(to_hash, raw=True)
    return hashlib.sha1(raw.encode(u'utf-8')).hexdigest()


def generate_pretty_slug(word_lists=(adjectives, adjectives, animals)):
    '''
    Generate a new slug using the adjective and animal lists available. The default word_lists value
    is a trio of lists: (adjectives, adjectives, animals). This produces 31,365,468 unique
    combinations which should be more than enough! This function does have the potential to
    produce duplicate adjectives (for example, green-green-llama) but the chances are really low and
    it doesn't really matter.

    :param word_lists: a sequence of word lists to choose from
    :return: the slug
    '''
    return u'{}-{}-{}'.format(*map(random.choice, word_lists))


def create_slug(context, query, query_version, version=None, resource_ids=None,
                resource_ids_and_versions=None, pretty_slug=True, attempts=5):
    '''
    Creates a new slug in the database and returns the saved DatastoreSlug object. If a slug already
    exists under the query hash then the existing slug entry is returned. In addition to the slug
    object, also returned is information about whether the slug was new or not.

    Only valid queries get a slug, otherwise we raise a ValidationError.

    Only valid resource ids included in the list will be stored, any invalid ones will be excluded.
    If a list of resource ids is provided and none of the requested resource ids are valid, then a
    ValidationError is raised.

    :param context: the context dict so that we can check the validity of any resources
    :param query: the query dict
    :param query_version: the query version in use
    :param version: the version to search at
    :param resource_ids: the resources to search (a list)
    :param resource_ids_and_versions: the resources and versions to search at (a dict)
    :param pretty_slug: whether to generate a pretty slug or just use the uuid id of the slug, by
                        default this is True
    :param attempts: how many times to try creating a pretty slug, default: 5
    :return: a 2-tuple containing a boolean indicating whether the slug object returned was newly
             created and the DatastoreSlug object itself. If we couldn't create a slug object for
             some reason then (False, None) is returned.
    '''
    # only store valid queries!
    validate_query(query, query_version)

    if resource_ids:
        resource_ids = list(utils.get_available_datastore_resources(context, resource_ids))
        if not resource_ids:
            raise toolkit.ValidationError(u"The requested resources aren't accessible to this user")

    query_hash = generate_query_hash(query, query_version, version, resource_ids,
                                     resource_ids_and_versions)

    existing_slug = model.Session.query(DatastoreSlug) \
        .filter(DatastoreSlug.query_hash == query_hash) \
        .first()

    if existing_slug is not None:
        return False, existing_slug

    while attempts:
        attempts -= 1
        new_slug = DatastoreSlug(query_hash=query_hash, query=query, query_version=query_version,
                                 version=version, resource_ids=resource_ids,
                                 resource_ids_and_versions=resource_ids_and_versions)

        if pretty_slug:
            new_slug.pretty_slug = u'visual-dark-ant'

        try:
            new_slug.save()
            break
        except IntegrityError:
            if pretty_slug:
                # assume this failed because of the pretty slug needing to be unique and try again
                model.Session.rollback()
                continue
            else:
                # something else has happened here
                raise
    else:
        return False, None

    return True, new_slug


def resolve_slug(slug):
    '''
    Resolves the given slug and returns it if it's found, otherwise None is returned.

    :param slug: the slug
    :return: a DatastoreSlug object or None if the slug couldn't be found
    '''
    return model.Session.query(DatastoreSlug) \
        .filter(or_(DatastoreSlug.id == slug, DatastoreSlug.pretty_slug == slug)) \
        .first()
