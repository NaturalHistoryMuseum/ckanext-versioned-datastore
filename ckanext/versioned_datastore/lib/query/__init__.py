import hashlib
import io
import json
import random
import uuid
from collections import OrderedDict

import abc
import dicthash
import itertools
import os
import redis
import six
from datetime import datetime
from eevee.search import create_version_query
from eevee.utils import to_timestamp
from jsonschema.validators import validator_for, RefResolver

from .slug_words import adjectives, animals
from .. import utils

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


def generate_query_id(query, query_version, version, resource_ids):
    '''
    Given a query and the parameters required to run it, create a unique id for it (a hash) and
    returns it. The hashing algorithm used is sha1 and the output will be a 40 character hexidecimal
    string.

    :param query: the query dict
    :param query_version: the query version
    :param version: the data version
    :param resource_ids: the ids of the resources under search
    :return: a unique id for the query, which is a hash of the query and parameters
    '''
    to_hash = {
        u'query': query,
        u'query_version': query_version,
        u'version': version,
        u'resource_ids': resource_ids,
    }
    raw = dicthash.generate_hash_from_dict(to_hash, raw=True)
    return hashlib.sha1(raw.encode(u'utf-8')).hexdigest()


def generate_slug(word_lists=(adjectives, adjectives, animals)):
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


def create_search_and_slug(query, query_version, version, resource_ids, ttl=60 * 60 * 24 * 7,
                           pretty_slug=True, attempts=10):
    '''
    Given the parameters required to create a query, validate it, translate it in an elasticsearch
    query and create a slug for it so that it can be referred to later.

    Unless a unique slug cannot be generated, a new slug is always returned even if the query has
    been seen before as this ensures individual slugs last only for their ttl even if accessed
    multiple times after creation. If a new slug cannot be generated then the query id is returned.
    If the query id is returned as the slug more than once from this function then the slug will
    last longer than the ttl, but this is an accepted risk (and a low one too).

    :param query: the query dict
    :param query_version: the query version
    :param version: the data version
    :param resource_ids: the ids of the resources under search
    :param ttl: the length of time a query slug should be valid for in seconds (default: 7 days)
    :param pretty_slug: whether to create a pretty slug or not. If this is turned off then UUID 4
                        slugs are returned. Default: True
    :param attempts: the number of attempts at generating a unique slug that are allowed, default 10
    :return: a 2-tuple containing the elasticsearch-dsl object and the slug
    '''
    # validate and translate the query into an elasticsearch-dsl Search object
    validate_query(query, query_version)
    search = translate_query(query, query_version)

    # create a unique id for the query and store it along with the info about it in redis
    query_id = generate_query_id(query, query_version, version, resource_ids)
    query_info = {
        u'query': query,
        u'query_version': query_version,
        u'version': version,
        u'resource_ids': list(resource_ids),
        u'search': search.to_dict(),
    }
    client = redis.Redis(host=u'localhost', port=6379, db=1)
    # store the query info against the query id and set the ttl, if the key already exists then the
    # effect of running this command is just that the ttl is reset (because the info should be the
    # same)
    client.setex(query_key_template.format(query_id), json.dumps(query_info), ttl)
    # store a slug using the query id
    client.setex(slug_key_template.format(query_id), query_id, ttl)

    # use the query id as the slug by default so that if we can't generate a slug we'll always be
    # able to return a slug to the caller
    slug = query_id

    slug_generator_function = generate_slug if pretty_slug else lambda: unicode(uuid.uuid4())
    while attempts:
        attempts -= 1
        slug = slug_generator_function()
        if client.setnx(slug_key_template.format(slug), query_id):
            client.expire(slug_key_template.format(slug), ttl)
            break

    # if the version wasn't provided, default it to now
    if version is None:
        version = to_timestamp(datetime.now())
    # add the version filter for the data to the search object
    search = search.filter(create_version_query(version))

    # prefix the resources to get the index names and add them to the search object
    search = search.index([utils.prefix_resource(resource_id) for resource_id in resource_ids])

    return search, slug


def resolve_slug(slug):
    '''
    Resolves the given slug and returns the query info stored against it.

    :param slug: the slug
    :return: a dict of query info or None if the slug couldn't be resolved
    '''
    client = redis.Redis(host=u'localhost', port=6379, db=1)
    query_id = client.get(u'slug:{}'.format(slug))
    if query_id:
        query_info = client.get(u'query:{}'.format(query_id))
        if query_info is not None:
            return json.loads(query_info)
    return None
