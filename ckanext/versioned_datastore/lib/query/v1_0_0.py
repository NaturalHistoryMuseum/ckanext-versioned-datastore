import hashlib
import io
import json
import string
from collections import defaultdict

from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Bool, Q

from .schema import Schema, load_core_schema, schema_base_path
from ..datastore_utils import prefix_field
from .utils import convert_small_or_groups, remove_empty_groups


class v1_0_0Schema(Schema):
    """
    Schema class for the v1.0.0 query schema.
    """

    version = 'v1.0.0'

    def __init__(self):
        self.schema, self.validator = load_core_schema(v1_0_0Schema.version)
        self.geojson = {
            'country': self.load_geojson(
                '50m-admin-0-countries-v4.1.0.geojson', ('NAME_EN', 'NAME')
            ),
            # if we use name_en we end up with one atlantic ocean whereas if we use name we get 2 -
            # the "North Atlantic Ocean" and the "South Atlantic Ocean". I think this is preferable.
            'marine': self.load_geojson('50m-marine-regions-v4.1.0.geojson', ('name',)),
            'geography': self.load_geojson(
                '50m-geography-regions-v4.1.0.geojson', ('name_en', 'name')
            ),
        }
        self.hasher = v1_0_0Hasher()

    def validate(self, query):
        """
        Validates the query against the v1.0.0 schema.

        :param query: the query to validate
        """
        self.validator.validate(query)

    def hash(self, query):
        """
        Hashes the given query and returns the hex digest of it.

        :param query: the query dict
        :return: the hex digest of the hash of the query
        """
        return self.hasher.hash_query(query)

    def translate(self, query, search=None):
        """
        Translates the query into an elasticsearch-dsl search object.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object to be built on instead of creating
                       a fresh object. By default a new search object is created.
        :return: an instantiated elasticsearch-dsl object
        """
        search = Search() if search is None else search
        search = self.add_search(query, search)
        search = self.add_filters(query, search)
        return search

    def normalise(self, query):
        """
        Corrects some (small) common query errors, e.g. removing empty groups.

        :param query: the query dict
        :return: the corrected/normalised query dict
        """
        query = convert_small_or_groups(query)
        query = remove_empty_groups(query)
        return query

    def add_search(self, query, search):
        """
        Adds a search to the search object and then returns it. Search terms map
        directly to the elasticsearch match query on the meta.all field. If there is no
        search in the query then the search object passed in is simply returned
        unaltered.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object
        :return: an instantiated elasticsearch-dsl object
        """
        if 'search' in query:
            return search.query(
                'match', **{'meta.all': {'query': query['search'], 'operator': 'and'}}
            )
        return search

    def add_filters(self, query, search):
        """
        Adds filters from the query into the search object and then returns it. If no
        filters are defined in the query then the search object passed in is simply
        returned unaltered.

        :param query: the whole query dict
        :param search: an instantiated elasticsearch-dsl object
        :return: an instantiated elasticsearch-dsl object
        """
        if 'filters' in query:
            return search.query(self.create_group_or_term(query['filters']))
        return search

    def create_group_or_term(self, group_or_term):
        """
        Creates and returns the elasticsearch-dsl query object necessary for the given
        group or term dict and returns it.

        :param group_or_term: a dict defining a single group or term
        :return: an elasticsearch-dsl object such as a Bool or Query object
        """
        # only one property is allowed so we can safely just extract the only name and options
        group_or_term_type, group_or_term_options = next(iter(group_or_term.items()))
        return getattr(self, f'create_{group_or_term_type}')(group_or_term_options)

    def create_and(self, group):
        """
        Creates and returns an elasticsearch-dsl query object representing the given
        group as an and query. This will be a Bool with a must in it for groups with
        more than 1 member, or will just be the actual member if only 1 member is found
        in the group. This is strictly unnecessary as elasticsearch/lucerne itself will
        normalise the query and remove redundant nestings but we might as well do it
        here seeing as we can and it makes smaller elasticsearch queries.

        :param group: the group to build the and from
        :return: the first member from the group if there's only one member in the group, or a Bool
        """
        members = [self.create_group_or_term(member) for member in group]
        return members[0] if len(members) == 1 else Bool(filter=members)

    def create_or(self, group):
        """
        Creates and returns an elasticsearch-dsl query object representing the given
        group as an or query. This will be a Bool with a should in it for groups with
        more than 1 member, or will just be the actual member if only 1 member is found
        in the group. This is strictly unnecessary as elasticsearch/lucerne itself will
        normalise the query and remove redundant nestings but we might as well do it
        here seeing as we can and it makes smaller elasticsearch queries.

        :param group: the group to build the or from
        :return: the first member from the group if there's only one member in the group, or a Bool
        """
        return self.build_or([self.create_group_or_term(member) for member in group])

    def create_not(self, group):
        """
        Creates and returns an elasticsearch-dsl query object representing the given
        group as a not query. This will be a Bool with a must_not in it.

        :param group: the group to build the not from
        :return: a Bool query
        """
        return Bool(must_not=[self.create_group_or_term(member) for member in group])

    def create_string_equals(self, options):
        """
        Given the options for a string_equals term, creates and returns an
        elasticsearch-dsl object to represent it. This term maps directly to an
        elasticsearch term query. If only one field is present in the fields property
        then the term query is returned directly, otherwise an or query is returned
        across all the fields requested.

        :param options: the options for the string_equals query
        :return: an elasticsearch-dsl Query object or a Bool object
        """
        return self.build_or(
            [
                Q('term', **{prefix_field(field): options['value']})
                for field in options['fields']
            ]
        )

    def create_string_contains(self, options):
        """
        Given the options for a string_contains term, creates and returns an
        elasticsearch-dsl object to represent it. This term maps directly to an
        elasticsearch match query on the .full subfield. If only one field is present in
        the fields property then the term query is returned directly, otherwise an or
        query is returned across all the fields requested.

        :param options: the options for the string_contains query
        :return: an elasticsearch-dsl Query object or a Bool object
        """
        fields = options['fields']
        query = {'query': options['value'], 'operator': 'and'}

        if fields:
            return self.build_or(
                [
                    Q('match', **{f'{prefix_field(field)}.full': query})
                    for field in fields
                ]
            )
        else:
            return Q('match', **{'meta.all': query})

    def create_number_equals(self, options):
        """
        Given the options for a number_equals term, creates and returns an
        elasticsearch-dsl object to represent it. This term maps directly to an
        elasticsearch term query. If only one field is present in the fields property
        then the term query is returned directly, otherwise an or query is returned
        across all the fields requested.

        :param options: the options for the number_equals query
        :return: an elasticsearch-dsl Query object or a Bool object
        """
        return self.build_or(
            [
                Q('term', **{f'{prefix_field(field)}.number': options['value']})
                for field in options['fields']
            ]
        )

    def create_number_range(self, options):
        """
        Given the options for a number_range term, creates and returns an elasticsearch-
        dsl object to represent it. This term maps directly to an elasticsearch range
        query. If only one field is present in the fields property then the term query
        is returned directly, otherwise an or query is returned across all the fields
        requested.

        :param options: the options for the number_range query
        :return: an elasticsearch-dsl Query object or a Bool object
        """
        less_than = options.get('less_than', None)
        greater_than = options.get('greater_than', None)
        less_than_inclusive = options.get('less_than_inclusive', True)
        greater_than_inclusive = options.get('greater_than_inclusive', True)
        query = {}
        if less_than is not None:
            query['lt' if not less_than_inclusive else 'lte'] = less_than
        if greater_than is not None:
            query['gt' if not greater_than_inclusive else 'gte'] = greater_than

        return self.build_or(
            [
                Q('range', **{f'{prefix_field(field)}.number': query})
                for field in options['fields']
            ]
        )

    def create_exists(self, options):
        """
        Given the options for an exists term, creates and returns an elasticsearch-dsl
        object to represent it. This term maps directly to an elasticsearch exists
        query. If only one field is present in the fields property then the term query
        is returned directly, otherwise an or query is returned across all the fields
        requested.

        :param options: the options for the exists query
        :return: an elasticsearch-dsl Query object or a Bool object
        """
        # TODO: should we provide exists on subfields?
        if options.get('geo_field', False):
            return Q('exists', field='meta.geo')
        else:
            return self.build_or(
                [Q('exists', field=prefix_field(field)) for field in options['fields']]
            )

    def create_geo_point(self, options):
        """
        Given the options for an geo_point term, creates and returns an elasticsearch-
        dsl object to represent it. This term maps directly to an elasticsearch
        geo_distance query. If only one field is present in the fields property then the
        term query is returned directly, otherwise an or query is returned across all
        the fields requested.

        :param options: the options for the geo_point query
        :return: an elasticsearch-dsl Query object or a Bool object
        """
        return Q(
            'geo_distance',
            **{
                'distance': f'{options.get("radius", 0)}{options.get("radius_unit", "m")}',
                'meta.geo': {
                    'lat': options['latitude'],
                    'lon': options['longitude'],
                },
            },
        )

    def create_geo_named_area(self, options):
        """
        Given the options for a geo_named_area term, creates and returns an
        elasticsearch-dsl object to represent it. This term maps directly to one or more
        elasticsearch geo_polygon queries, if necessary combined using ands, ors and
        nots to provide MultiPolygon hole support.

        In v1.0.0, Natural Earth Data datasets are used to provide the lists of names and
        corresponding geojson areas. The 1:50million scale is used in an attempt to provide a good
        level of detail without destroying Elasticsearch with enormous numbers of points. See the
        `theme/public/querySchemas/geojson/` directory for source data and readme, and also the
        load_geojson function in this class.

        :param options: the options for the geo_named_area query
        :return: an elasticsearch-dsl Query object (a single geo_polygon Query or a Bool Query)
        """
        category, name = next(iter(options.items()))
        return self.build_multipolygon_query(self.geojson[category][name])

    def create_geo_custom_area(self, coordinates):
        """
        Given the coordinates for a geo_custom_area term, creates and returns an
        elasticsearch-dsl object to represent it. This term takes the equivalent of the
        coordinates array from a MultiPolygon type feature in GeoJSON and uses it to
        build a query which captures records that fall in the polygon (and outside any
        holes defined in the Polygon).

        :param coordinates: a MultiPolygon coordinates list
        :return: an elasticsearch-dsl Query object (a single geo_polygon Query or a Bool Query)
        """
        return self.build_multipolygon_query(coordinates)

    @staticmethod
    def build_or(terms):
        """
        Utility function which when given a list of elasticsearch-dsl query objects,
        either returns the first one on it's own or creates an "or" query encapsulating
        them.

        :param terms: a list of elasticsearch-dsl terms
        :return: either a Query object or a Bool should object
        """
        return (
            terms[0] if len(terms) == 1 else Bool(should=terms, minimum_should_match=1)
        )

    @staticmethod
    def build_geo_polygon_query(points):
        """
        Given a list of points (where each point is a list with 2 elements, the
        longitude and the latitude (note the order, it's the same as the GeoJSON spec)),
        creates a geo_polygon elasticsearch-dsl query object for the points and returns
        it.

        :param points: a list of points
        :return: an elasticsearch-dsl query object
        """
        return Q(
            'geo_polygon',
            **{
                'meta.geo': {
                    'points': [{'lat': point[1], 'lon': point[0]} for point in points]
                }
            },
        )

    @staticmethod
    def build_multipolygon_query(coordinates):
        """
        Utility function for building elasticsearch-dsl queries that represent GeoJSON
        MultiPolygons. Given the coordinates this function creates a geo_polygon queries
        and Bool queries to represent the varioud enclosures and holes in those
        enclosures to find all records residing in the MultiPolygon. The coordinates
        parameter should match the format required by GeoJSON and therefore be a series
        of nested lists, see the GeoJSON docs for details.

        :param coordinates: the coordinate list, which is basically a list of Polygons. See the
                            GeoJSON doc for the exact format and meaning
        :return: an elasticsearch-dsl object representing the MultiPolygon
        """
        queries = []
        # the first list is a list of GeoJSON Polygons
        for polygon in coordinates:
            # then the Polygon is a list containing at least one element. The first element is the
            # outer boundary shape of the polygon and any other elements are holes in this shape
            outer, holes = polygon[0], polygon[1:]
            outer_query = v1_0_0Schema.build_geo_polygon_query(outer)

            if holes:
                holes_queries = [
                    v1_0_0Schema.build_geo_polygon_query(hole) for hole in holes
                ]
                # create a query which filters the outer query but filters out the holes
                queries.append(Bool(filter=[outer_query], must_not=holes_queries))
            else:
                queries.append(outer_query)

        return v1_0_0Schema.build_or(queries)

    @staticmethod
    def load_geojson(filename, name_keys):
        """
        Load the given geojson file, build a lookup using the data and the name_keys
        parameter and return it.

        The geojson file is assumed to be a list of features containing only Polygon or
        MultiPolygon types.

        The name_keys parameter should be a sequence of keys to use to retrieve a name for the
        feature from the properties dict. The first key found in the properties dict with a value is
        used and therefore the keys listed should be in priority order. The extracted name is passed
        to string.capwords to produce a sensible and consistent set of names.

        :param filename: the name geojson file to load from the given path
        :param name_keys: a priority ordered sequence of keys to use for feature name retrieval
        :return: a dict of names -> MultiPolygons
        """
        path = schema_base_path.joinpath(v1_0_0Schema.version).joinpath('geojson')

        # make sure we read the file using utf-8
        with io.open(path.joinpath(filename), 'r', encoding='utf-8') as f:
            lookup = defaultdict(list)
            for feature in json.load(f)['features']:
                # find the first name key with a value and pass it to string.capwords
                name = string.capwords(
                    next(
                        iter(
                            filter(
                                None,
                                (
                                    feature['properties'].get(key, None)
                                    for key in name_keys
                                ),
                            )
                        )
                    )
                )

                coordinates = feature['geometry']['coordinates']
                # if the feature is a Polygon, wrap it in a list to make it a MultiPolygon
                if feature['geometry']['type'] == 'Polygon':
                    coordinates = [coordinates]

                # add the polygons found to the existing MultiPolygon (some names are listed
                # multiple times in the source geojson files and require stitching together to make
                # a single name -> MultiPolygon mapping
                for polygon in coordinates:
                    # if a polygon is already represented in the MultiPolygon, ignore the dupe
                    if polygon not in lookup[name]:
                        lookup[name].append(polygon)

            return lookup


class v1_0_0Hasher:
    """
    Query hasher class for the v1.0.0 query schema.
    """

    def hash_query(self, query):
        """
        Stable hash function for v1.0.0 queries.

        :param query: the query dict
        :return: the hex digest
        """
        query_hash = hashlib.sha1()
        if 'search' in query:
            query_hash.update(f'search:{query["search"]}'.encode(u'utf-8'))
        if 'filters' in query:
            data = f'filters:{self.create_group_or_term(query["filters"])}'.encode(
                u'utf-8'
            )
            query_hash.update(data)
        return query_hash.hexdigest()

    def create_group_or_term(self, group_or_term):
        """
        Creates and returns a string version of the given group or term dict and returns
        it.

        :param group_or_term: a dict defining a single group or term
        :return: a string representing the group or term
        """
        # only one property is allowed so we can safely just extract the only name and options
        group_or_term_type, group_or_term_options = next(iter(group_or_term.items()))
        return getattr(self, f'create_{group_or_term_type}')(group_or_term_options)

    def create_and(self, group):
        """
        Creates and returns a string version of the given group as an and query.

        :param group: the group to build the and from
        :return: a string representing the group
        """
        # sorting the members makes this stable
        members = sorted(self.create_group_or_term(member) for member in group)
        return f'and:[{"|".join(members)}]'

    def create_or(self, group):
        """
        Creates and returns a string version of the given group as an or query.

        :param group: the group to build the or from
        :return: a string representing the group
        """
        # sorting the members makes this stable
        members = sorted(self.create_group_or_term(member) for member in group)
        return f'or:[{"|".join(members)}]'

    def create_not(self, group):
        """
        Creates and returns a string version of the given group as a not query.

        :param group: the group to build the not from
        :return: a string representing the group
        """
        # sorting the members makes this stable
        members = sorted(self.create_group_or_term(member) for member in group)
        return f'not:[{"|".join(members)}]'

    @staticmethod
    def create_string_equals(options):
        """
        Given the options for a string_equals term, creates and returns a string version
        of it.

        :param options: the options for the string_equals query
        :return: a string representing the term
        """
        # sorting the fields makes this stable
        fields = ','.join(sorted(options['fields']))
        return f'string_equals:{fields};{options["value"]}'

    @staticmethod
    def create_string_contains(options):
        """
        Given the options for a string_contains term, creates and returns a string
        version of it.

        :param options: the options for the string_contains query
        :return: a string representing the term
        """
        # sorting the fields makes this stable
        fields = ','.join(sorted(options['fields']))
        return f'string_contains:{fields};{options["value"]}'

    @staticmethod
    def create_number_equals(options):
        """
        Given the options for a number_equals term, creates and returns a string version
        of it.

        :param options: the options for the number_equals query
        :return: a string representing the term
        """
        # sorting the fields makes this stable
        fields = ','.join(sorted(options['fields']))
        return f'number_equals:{fields};{options["value"]}'

    @staticmethod
    def create_number_range(options):
        """
        Given the options for a number_range term, creates and returns a string version
        of it.

        :param options: the options for the number_range query
        :return: a string representing the term
        """
        # sorting the fields makes this stable
        fields = ','.join(sorted(options['fields']))
        hash_value = f'number_range:{fields};'

        less_than = options.get('less_than', None)
        less_than_inclusive = options.get('less_than_inclusive', True)
        if less_than is not None:
            hash_value += '<'
            if less_than_inclusive:
                hash_value += '='
            hash_value += str(less_than)

        greater_than = options.get('greater_than', None)
        greater_than_inclusive = options.get('greater_than_inclusive', True)
        if greater_than is not None:
            hash_value += '>'
            if greater_than_inclusive:
                hash_value += '='
            hash_value += str(greater_than)

        return hash_value

    @staticmethod
    def create_exists(options):
        """
        Given the options for a exists term, creates and returns a string version of it.

        :param options: the options for the exists query
        :return: a string representing the term
        """
        if options.get('geo_field', False):
            return 'geo_exists'
        else:
            # sorting the fields makes this stable
            fields = ','.join(sorted(options['fields']))
            return f'exists:{fields}'

    @staticmethod
    def create_geo_point(options):
        """
        Given the options for a geo_point term, creates and returns a string version of
        it.

        :param options: the options for the geo_point query
        :return: a string representing the term
        """
        distance = f'{options.get("radius", 0)}{options.get("radius_unit", "m")}'
        return f'geo_point:{distance};{options["latitude"]};{options["longitude"]}'

    @staticmethod
    def create_geo_named_area(options):
        """
        Given the options for a geo_named_area term, creates and returns a string
        version of it.

        :param options: the options for the geo_named_area query
        :return: a string representing the term
        """
        return 'geo_named_area:{};{}'.format(*next(iter(options.items())))

    def create_geo_custom_area(self, coordinates):
        """
        Given the coordinates for a geo_custom_area term, creates and returns a string
        version of it.

        :param coordinates: the coordinates for the geo_custom_area query
        :return: a string representing the term
        """
        queries = []
        # the first list is a list of GeoJSON Polygons
        for polygon in coordinates:
            # then the Polygon is a list containing at least one element. The first element is the
            # outer boundary shape of the polygon and any other elements are holes in this shape
            outer, holes = polygon[0], polygon[1:]
            outer_query = self.build_geo_polygon_query(outer)

            if holes:
                # sort the holes to ensure stability
                holes_queries = sorted(
                    self.build_geo_polygon_query(hole) for hole in holes
                )
                # create a query which filters the outer query but filters out the holes
                queries.append(f'{outer_query}/{holes_queries}')
            else:
                queries.append(outer_query)

        return f'geo_custom_area:{";".join(queries)}'

    @staticmethod
    def build_geo_polygon_query(points):
        """
        Given a series of points, returns a string version of them. Note that we don't
        sort them as that could change the meaning.

        :param points: the points as lat lon pairs
        :return: a string representing the points
        """
        return ','.join(f'[{point[1]},{point[0]}]' for point in points)
