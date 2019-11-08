import io
import json
import string
from collections import defaultdict

import os
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.query import Schema, load_core_schema, schema_base_path
from ckanext.versioned_datastore.lib.utils import prefix_field
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Bool, Q


class v1_0_0Schema(Schema):
    '''
    Schema class for the v1.0.0 query schema.
    '''

    version = u'v1.0.0'

    def __init__(self):
        self.schema, self.validator = load_core_schema(v1_0_0Schema.version)
        self.geojson = {
            u'country': self.load_geojson(u'50m-admin-0-countries-v4.1.0.geojson',
                                          (u'NAME_EN', u'NAME')),
            # if we use name_en we end up with one atlantic ocean whereas if we use name we get 2 -
            # the "North Atlantic Ocean" and the "South Atlantic Ocean". I think this is preferable.
            u'marine': self.load_geojson(u'50m-marine-regions-v4.1.0.geojson', (u'name',)),
            u'geography': self.load_geojson(u'50m-geography-regions-v4.1.0.geojson',
                                            (u'name_en', u'name')),
        }

    def validate(self, query):
        '''
        Validates the query against the v1.0.0 schema.

        :param query: the query to validate
        '''
        self.validator.validate(query)

    def translate(self, query, search=None):
        '''
        Translates the query into an elasticsearch-dsl search object.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object to be built on instead of creating
                       a fresh object. By default a new search object is created.
        :return: an instantiated elasticsearch-dsl object
        '''
        search = Search() if search is None else search
        search = self.add_search(query, search)
        search = self.add_filters(query, search)
        return search

    def add_search(self, query, search):
        '''
        Adds a search to the search object and then returns it. Search terms map directly to the
        elasticsearch match query on the meta.all field. If there is no search in the query then the
        search object passed in is simply returned unaltered.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object
        :return: an instantiated elasticsearch-dsl object
        '''
        if u'search' in query:
            return search.query(u'match', **{u'meta.all': {u'query': query[u'search'],
                                                           u'operator': u'and'}})
        return search

    def add_filters(self, query, search):
        '''
        Adds filters from the query into the search object and then returns it. If no filters are
        defined in the query then the search object passed in is simply returned unaltered.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object
        :return: an instantiated elasticsearch-dsl object
        '''
        if u'filters' in query:
            return search.query(self.create_group_or_term(query[u'filters']))
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
        return members[0] if len(members) == 1 else Bool(filter=members)

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

    def create_geo_named_area(self, options):
        '''
        Given the options for a geo_named_area term, creates and returns an elasticsearch-dsl object
        to represent it. This term maps directly to one or more elasticsearch geo_polygon queries,
        if necessary combined using ands, ors and nots to provide MultiPolygon hole support.

        In v1.0.0, Natural Earth Data datasets are used to provide the lists of names and
        corresponding geojson areas. The 1:50million scale is used in an attempt to provide a good
        level of detail without destroying Elasticsearch with enormous numbers of points. See the
        `theme/public/querySchemas/geojson/` directory for source data and readme, and also the
        load_geojson function in this class.

        :param options: the options for the geo_named_area query
        :return: an elasticsearch-dsl Query object or a Bool object
        '''
        queries = []
        category, name = next(iter(options.items()))
        for polygon in self.geojson[category][name]:
            outer, holes = polygon[0], polygon[1:]
            outer_query = self.build_polygon_query(outer, validate=False)

            if holes:
                holes_queries = [self.build_polygon_query(hole, validate=False) for hole in holes]
                # create a query which filters the outer query but filters out the holes
                queries.append(Bool(filter=[outer_query], must_not=holes_queries))
            else:
                queries.append(outer_query)

        return self.build_or(queries)

    @staticmethod
    def build_or(terms):
        '''
        Utility function which when given a list of elasticsearch-dsl query objects, either returns
        the first one on it's own or creates an "or" query encapsulating them.

        :param terms: a list of elasticsearch-dsl terms
        :return: either a Query object or a Bool should object
        '''
        return terms[0] if len(terms) == 1 else Bool(should=terms, minimum_should_match=1)

    @staticmethod
    def build_polygon_query(points, validate=True):
        '''
        Utility function which when given a list of points, creates a geo_polygon query and returns
        it. The points list must contain at least 4 lists, each list representing the longitude and
        latitude of the point (note the order, this comes from the geojson standard). If fewer than
        4 points are passed then a ValidationError is thrown. The first and last points in the list
        must be the same, otherwise a ValidationError is thrown.

        :param points: the list of points
        :param validate: whether to validate the points for correctness against the geojson spec,
                         default: True
        :return: a geo_polygon query object
        '''
        if validate:
            if len(points) < 4:
                raise toolkit.ValidationError(u'Not enough points')

            if points[0] != points[-1]:
                raise toolkit.ValidationError(u'First and last point must be the same')

        return Q(u'geo_polygon', **{
            u'meta.geo': {
                u'points': [{u'lat': point[1], u'lon': point[0]} for point in points]
            }
        })

    @staticmethod
    def load_geojson(filename, name_keys):
        '''
        Load the given geojson file, build a lookup using the data and the name_keys parameter and
        return it.

        The geojson file is assumed to be a list of features containing only Polygon or
        MultiPolygon types.

        The name_keys parameter should be a sequence of keys to use to retrieve a name for the
        feature from the properties dict. The first key found in the properties dict with a value is
        used and therefore the keys listed should be in priority order. The extracted name is passed
        to string.capwords to produce a sensible and consistent set of names.

        :param filename: the name geojson file to load from the given path
        :param name_keys: a priority ordered sequence of keys to use for feature name retrieval
        :return: a dict of names -> MultiPolygons
        '''
        path = os.path.join(schema_base_path, v1_0_0Schema.version, u'geojson')

        # make sure we read the file using utf-8
        with io.open(os.path.join(path, filename), u'r', encoding=u'utf-8') as f:
            lookup = defaultdict(list)
            for feature in json.load(f)[u'features']:
                # find the first name key with a value and pass it to string.capwords
                name = string.capwords(next(iter(
                    filter(None, (feature[u'properties'].get(key, None) for key in name_keys)))))

                coordinates = feature[u'geometry'][u'coordinates']
                # if the feature is a Polygon, wrap it in a list to make it a MultiPolygon
                if feature[u'geometry'][u'type'] == u'Polygon':
                    coordinates = [coordinates]

                # add the polygons found to the existing MultiPolygon (some names are listed
                # multiple times in the source geojson files and require stitching together to make
                # a single name -> MultiPolygon mapping
                for polygon in coordinates:
                    # if a polygon is already represented in the MultiPolygon, ignore the dupe
                    if polygon not in lookup[name]:
                        lookup[name].append(polygon)

            return lookup
