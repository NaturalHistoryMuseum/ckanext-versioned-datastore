import json
from collections import OrderedDict

import os
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Bool, Q

from ckanext.versioned_datastore.lib.utils import prefix_field
from jsonschema.validators import validator_for


def load_schemas(schema_base_path=None):
    '''
    Loads the available schemas from the theme/public/schemas directory and returns a dict of
    versions -> Draft7Validators.

    Schema files are expected to be named v#.#.#.json.

    :param schema_base_path: the path to load the schemas from, by default theme/public/schemas is
                             used
    :return: an OrderedDict of versions -> Draft7Validator objects in ascending version order
    '''
    loaded_schemas = []

    if schema_base_path is None:
        parent_path = os.path.dirname(__file__)
        schema_base_path = os.path.join(parent_path, u'..', u'theme', u'public', u'schemas')

    for schema_file in os.listdir(schema_base_path):
        if schema_file.endswith(u'.json'):
            with open(os.path.join(schema_base_path, schema_file), u'r') as f:
                version = schema_file[:-5]

                # load the schema json and pick the correct validator based on the schema's defined
                # version in $schema
                schema = json.load(f)
                validator_cls = validator_for(schema)
                validator_cls.check_schema(schema)
                validator = validator_cls(schema)

                loaded_schemas.append((version, validator))

    # sort the schemas by version in ascending order (h/t https://stackoverflow.com/a/2574090)
    loaded_schemas.sort(key=lambda vs: [int(u) for u in vs[0][1:].split(u'.')])
    return OrderedDict(loaded_schemas)


schemas = load_schemas()


class InvalidQuerySchemaVersionError(Exception):

    def __init__(self, version):
        super(Exception, self).__init__(u'Invalid query version: {}'.format(version))


def get_latest_query_version():
    '''
    Retrieve the latest query version from the schemas OrderedDict. This relies on the schemas dict
    maintaining an ascending sort order on version.

    :return: the latest available query schema version
    '''
    return next(iter(schemas.keys()))


def validate_query(query, query_version):
    '''
    Validates the query against the query_version.

    :param query: the query to validate
    :param query_version: the query version to validate against
    :return: True if the query is valid, otherwise an InvalidQuerySchemaVersionError is raised
    '''
    if query_version not in schemas:
        raise InvalidQuerySchemaVersionError(query_version)
    schemas[query_version].validate(query)
    return True


def translate_query(query, query_version):
    '''
    Translates the given query into an elasticsearch-dsl search object. The query_version is used
    to translate the query correctly.

    :param query: the query to translate
    :param query_version: the query version to translate against
    :return: an instantiated elasticsearch-dsl object
    '''
    if query_version == u'v1.0.0':
        return v1_0_0Translator(query).translate()
    else:
        raise InvalidQuerySchemaVersionError(query_version)


class v1_0_0Translator(object):
    '''
    Translator for the v1.0.0 query schema.
    '''

    def __init__(self, query):
        '''
        :param query: the query dict
        '''
        self.query = query

    def translate(self, search=None):
        '''
        Translates the query into an elasticsearch-dsl search object.

        :param search: an instantiated elasticsearch-dsl object to be built on instead of creating
                       a fresh object. By default a new search object is created.
        :return: an instantiated elasticsearch-dsl object
        '''
        search = Search() if search is None else search
        search = self.add_search(search)
        search = self.add_filters(search)
        return search

    def add_search(self, search):
        '''
        Adds a search to the search object and then returns it. Search terms map directly to the
        elasticsearch match query on the meta.all field. If there is no search in the query then the
        search object passed in is simply returned unaltered.

        :param search: an instantiated elasticsearch-dsl object
        :return: an instantiated elasticsearch-dsl object
        '''
        if u'search' in self.query:
            search = search.query(u'match', **{u'meta.all': {u'query': self.query[u'search'],
                                                             u'operator': u'and'}})
        return search

    def add_filters(self, search):
        '''
        Adds filters from the query into the search object and then returns it. If no filters are
        defined in the query then the search object passed in is simply returned unaltered.

        :param search: an instantiated elasticsearch-dsl object
        :return: an instantiated elasticsearch-dsl object
        '''
        if u'filters' in self.query:
            search = search.query(self.create_group_or_term(self.query[u'filters']))
        return search

    def create_group_or_term(self, group_or_term):
        '''
        Creates and returns the elasticsearch-dsl query object necessary for the given group or
        term dict and returns it.

        :param group_or_term: a dict defining a single group or term
        :return: an elasticsearch-dsl object such as a Bool or Query object
        '''
        # only one property is allowed so we can safely just extract the only name and options
        group_or_term_type, group_or_term_options = next(iter(group_or_term.items()))
        return getattr(self, u'create_{}'.format(group_or_term_type))(group_or_term_options)

    def create_and(self, group):
        '''
        Creates and returns an elasticsearch-dsl query object representing the given group as an
        and query. This will be a Bool with a must in it for groups with more than 1 member, or will
        just be the actual member if only 1 member is found in the group. This is strictly
        unnecessary as elasticsearch/lucerne itself will normalise the query and remove redundant
        nestings but we might as well do it here seeing as we can and it makes smaller elasticsearch
        queries.

        :param group: the group to build the and from
        :return: the first member from the group if there's only one member in the group, or a Bool
        '''
        members = [self.create_group_or_term(member) for member in group]
        return members[0] if len(members) == 1 else Bool(must=members)

    def create_or(self, group):
        '''
        Creates and returns an elasticsearch-dsl query object representing the given group as an
        or query. This will be a Bool with a should in it for groups with more than 1 member, or
        will just be the actual member if only 1 member is found in the group. This is strictly
        unnecessary as elasticsearch/lucerne itself will normalise the query and remove redundant
        nestings but we might as well do it here seeing as we can and it makes smaller elasticsearch
        queries.

        :param group: the group to build the or from
        :return: the first member from the group if there's only one member in the group, or a Bool
        '''
        return self.build_or([self.create_group_or_term(member) for member in group])

    def build_or(self, terms):
        '''
        Utility function which given a list of elasticsearch-dsl query objects, either returns the
        first one on it's own or creates an "or" query encapsulating them.

        :param terms: a list of elasticsearch-dsl terms
        :return: either a Query object or a Bool should object
        '''
        return terms[0] if len(terms) == 1 else Bool(should=terms, minimum_should_match=1)

    def create_string_equals(self, options):
        '''
        Given the options for a string_equals term, creates and returns an elasticsearch-dsl object
        to represent it. This term maps directly to an elasticsearch term query. If only one field
        is present in the fields property then the term query is returned directly, otherwise an or
        query is returned across all the fields requested.

        :param options: the options for the string_equals query
        :return: an elasticsearch-dsl Query object or a Bool object
        '''
        return self.build_or([Q(u'term', **{prefix_field(field): options[u'value']})
                              for field in options[u'fields']])

    def create_string_contains(self, options):
        '''
        Given the options for a string_contains term, creates and returns an elasticsearch-dsl
        object to represent it. This term maps directly to an elasticsearch match query on the .full
        subfield. If only one field is present in the fields property then the term query is
        returned directly, otherwise an or query is returned across all the fields requested.

        :param options: the options for the string_contains query
        :return: an elasticsearch-dsl Query object or a Bool object
        '''
        fields = options[u'fields']
        query = {u'query': options[u'value'], u'operator': u'and'}

        if fields:
            return self.build_or([Q(u'match', **{u'{}.full'.format(prefix_field(field)): query})
                                  for field in fields])
        else:
            return Q(u'match', **{u'meta.all': query})

    def create_number_equals(self, options):
        '''
        Given the options for a number_equals term, creates and returns an elasticsearch-dsl object
        to represent it. This term maps directly to an elasticsearch term query. If only one field
        is present in the fields property then the term query is returned directly, otherwise an or
        query is returned across all the fields requested.

        :param options: the options for the number_equals query
        :return: an elasticsearch-dsl Query object or a Bool object
        '''
        return self.build_or(
            [Q(u'term', **{u'{}.number'.format(prefix_field(field)): options[u'value']})
             for field in options[u'fields']])

    def create_number_range(self, options):
        '''
        Given the options for a number_range term, creates and returns an elasticsearch-dsl object
        to represent it. This term maps directly to an elasticsearch range query. If only one field
        is present in the fields property then the term query is returned directly, otherwise an or
        query is returned across all the fields requested.

        :param options: the options for the number_range query
        :return: an elasticsearch-dsl Query object or a Bool object
        '''
        less_than = options.get(u'less_than', None)
        greater_than = options.get(u'greater_than', None)
        less_than_inclusive = options.get(u'less_than_inclusive', True)
        greater_than_inclusive = options.get(u'greater_than_inclusive', True)
        query = {}
        if less_than is not None:
            query[u'lt' if not less_than_inclusive else u'lte'] = less_than
        if greater_than is not None:
            query[u'gt' if not greater_than_inclusive else u'gte'] = greater_than

        return self.build_or([Q(u'range', **{u'{}.number'.format(prefix_field(field)): query})
                             for field in options[u'fields']])

    def create_exists(self, options):
        '''
        Given the options for an exists term, creates and returns an elasticsearch-dsl object to
        represent it. This term maps directly to an elasticsearch exists query. If only one field
        is present in the fields property then the term query is returned directly, otherwise an or
        query is returned across all the fields requested.

        :param options: the options for the exists query
        :return: an elasticsearch-dsl Query object or a Bool object
        '''
        # TODO: should we provide exists on subfields?
        if options.get(u'geo_field', False):
            return Q(u'exists', field=u'meta.geo')
        else:
            return self.build_or([Q(u'exists', field=prefix_field(field))
                                 for field in options[u'fields']])

    def create_geo_point(self, options):
        '''
        Given the options for an geo_point term, creates and returns an elasticsearch-dsl object to
        represent it. This term maps directly to an elasticsearch geo_distance query. If only one
        field is present in the fields property then the term query is returned directly, otherwise
        an or query is returned across all the fields requested.

        :param options: the options for the geo_point query
        :return: an elasticsearch-dsl Query object or a Bool object
        '''
        return Q(u'geo_distance', **{
            u'distance': u'{}{}'.format(options.get(u'radius', 0),
                                        options.get(u'radius_unit', u'm')),
            u'meta.geo': {
                u'lat': options[u'latitude'],
                u'lon': options[u'longitude'],
            }
        })
