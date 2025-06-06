import abc
import json
from operator import itemgetter
from typing import Any, List, Optional, Union

from ckan.plugins import toolkit
from elasticsearch_dsl import Q
from elasticsearch_dsl.query import EMPTY_QUERY, Bool
from elasticsearch_dsl.query import Query as DSLQuery
from splitgill.search import ALL_POINTS, match_query, term_query

from ckanext.versioned_datastore.lib.query.schema import (
    get_latest_query_version,
    hash_query,
    normalise_query,
    translate_query,
    validate_query,
)


class Query(abc.ABC):
    """
    Abstract base class representing a query at a specific version over a specific set
    of resources.

    Subclasses should override the to_dsl method to provide customised filtering
    functionality.
    """

    def __init__(
        self, kind: str, resource_ids: List[str], version: Optional[int] = None
    ):
        """
        :param kind: an identifier for the kind of query this is
        :param resource_ids: the IDs of the resources to search
        :param version: the version at which to search
        """
        self.kind = kind
        self.resource_ids = resource_ids
        self.version = version

    @abc.abstractmethod
    def to_dsl(self) -> DSLQuery:
        """
        Returns just an Elasticsearch DSL Query object representing this search.

        :returns: an Elasticsearch Query instance
        """
        ...

    def __eq__(self, other: Any):
        if isinstance(other, Query):
            return self.to_dsl() == other.to_dsl()
        return False


class DirectQuery(Query):
    """
    A query created using an Elasticsearch DSL Query object.
    """

    def __init__(
        self,
        resource_ids: List[str],
        version: Optional[int] = None,
        dsl_query: Optional[DSLQuery] = None,
    ):
        """
        :param resource_ids: the IDs of the resources to search
        :param version: the version to search at
        :param dsl_query: the query to perform
        """
        super().__init__('direct', resource_ids, version)
        self.dsl = dsl_query if dsl_query is not None else EMPTY_QUERY

    def to_dsl(self) -> DSLQuery:
        return self.dsl


class SchemaQuery(Query):
    """
    A query using the schemas registered with this plugin.
    """

    def __init__(
        self,
        resource_ids: List[str],
        version: Optional[int] = None,
        query: Optional[dict] = None,
        query_version: Optional[str] = None,
    ):
        """
        :param resource_ids: the IDs of the resources to search
        :param version: the version to search at
        :param query: the query to use (defaults to {} if not given)
        :param query_version: the query version to use (defaults to latest if not given)
        """
        super().__init__('schema', resource_ids, version)
        self.query_version = (
            query_version if query_version else get_latest_query_version()
        )
        if query:
            self.query = normalise_query(query, self.query_version)
        else:
            self.query = {}

    @property
    def hash(self) -> str:
        """
        Returns the hash of the query for use in identifying it uniquely.

        :returns: the hash as a str
        """
        return hash_query(self.query, self.query_version)

    def validate(self):
        """
        Checks that the query is valid according to the schema.

        If valid, nothing happens, if not valid, an error will be raised.
        """
        validate_query(self.query, self.query_version)

    def to_dsl(self) -> DSLQuery:
        """
        Checks that the query is valid according to the schema and then translates it to
        a Search object.

        :returns: a Search object
        """
        self.validate()
        return translate_query(self.query, self.query_version)


class BasicQuery(Query):
    """
    A query using the old CKAN datastore style query method (with a couple of minor
    differences).
    """

    def __init__(
        self,
        resource_id: str,
        version: Optional[int] = None,
        q: Optional[Union[str, dict]] = None,
        filters: Optional[dict] = None,
    ):
        """
        :param resource_id: the ID of the resource to search
        :param version: the version to search at
        :param q: a query string which will be searched against the all text field or a
                  dict of fields and search values. If this is a dict then the keys
                  are understood as field names, but if a key is an empty string then
                  the all text field is searched. This allows combination searches
                  across all text and specific fields.
        :param filters: a dict of fields and values to filter the result with. If a key
                        is present that is equal to "__geo__" then the value associated
                        with it should be a dict which will be treated as a geo query to
                        be run against the all points field. The value should contain a
                        "type" key which must have a corresponding value of "point",
                        "box" or "polygon" and then other keys that are dependent on the
                        type:
                          - point:
                            - distance: the radius of the circle centred on the
                                        specified location within which records must lie
                                        to be matched. This can specified in any form
                                        that Elasticsearch accepts for distances (see
                                        their doc, but values like 10km etc).
                            - point: the point to centre the radius on, specified as a
                                     lat, long pair in a list (i.e. [-20, 40.2]).
                        - box:
                          - points: the top left and bottom right points of the box,
                                    specified as a list of two lat/long pairs
                                    (i.e. [[-20, 40.2], [0.5, 100]]).
                        - polygon:
                          - points: a list of at least 3 lat/long pairs
                                    (i.e. [[-1, 4], [-1.1, 4.8], [1.99, 35], [5, 49]]).
        """
        super().__init__('basic', [resource_id], version)
        self.q = q
        self.filters = filters

    def to_dsl(self) -> DSLQuery:
        """
        Converts the q and filters parameters specified on creation to an Elasticsearch
        DSL query.

        :returns: the Elasticsearch DSL query
        """
        must = []
        if self.q is not None and self.q != '' and self.q != {}:
            if isinstance(self.q, (str, int, float)):
                # add a free text query across all fields
                must.append(match_query(str(self.q), operator='and'))
            elif isinstance(self.q, dict):
                for field, query in sorted(self.q.items(), key=itemgetter(0)):
                    must.append(
                        match_query(
                            str(query),
                            # todo: use __all__ instead of empty str to match __geo__?
                            field=None if field == '' else field,
                            operator='and',
                        )
                    )

        filters = []
        if self.filters is not None:
            for field, values in sorted(self.filters.items(), key=itemgetter(0)):
                if not isinstance(values, list):
                    values = [values]
                if field == '__geo__':
                    # only pass through the first value
                    filters.append(BasicQuery.create_geo_filter(values[0]))
                else:
                    for value in values:
                        filters.append(term_query(field, str(value)))

        # return the cleanest version of the search we can
        if not must and not filters:
            return EMPTY_QUERY
        elif must and not filters:
            return must[0] if len(must) == 1 else Bool(must=must)
        elif not must and filters:
            return filters[0] if len(filters) == 1 else Bool(filter=filters)
        else:
            return Bool(must=must, filter=filters)

    @staticmethod
    def create_point_filter(distance: str, coordinates: list) -> DSLQuery:
        """
        Creates an Elasticsearch Query object filtering for records within the radius of
        the given point coordinates.

        :param distance: the radius of the circle centred on the specified location
            within which records must lie to be matched. This can be specified in any
            form that elasticsearch accepts for distances (see their doc, but
            essentially values like 10km etc).
        :param coordinates: the point to centre the radius on, specified as a lon/lat
            pair in a list (i.e. [40.2, -20]).
        :returns: a Query object
        """
        options = {
            'distance': distance,
            ALL_POINTS: {
                'lat': float(coordinates[1]),
                'lon': float(coordinates[0]),
            },
        }
        return Q('geo_distance', **options)

    @staticmethod
    def create_multipolygon_filter(coordinates: list) -> DSLQuery:
        """
        Creates a Query object for the given multipolygon filters. Only the first group
        in each polygon grouping will be used as elasticsearch doesn't support this type
        of query yet (this kind of query is used for vacating space inside a polygon,
        like a donut for example).

        If more than one group is included then they are included as an or with a
        minimum must match of 1.

        :param coordinates: a list of a list of a list of a list of at least 3 lon/lat
            pairs (i.e. [[[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]]])
        :returns: a Query object
        """
        should = []
        for group in coordinates:
            points = group[0]
            if len(points) < 3:
                raise toolkit.ValidationError(
                    'Not enough points in the polygon, must be 3 or more'
                )

            options = {
                ALL_POINTS: {
                    'shape': {
                        'type': 'Polygon',
                        # format the polygon point data appropriately
                        'coordinates': [
                            [[float(point[0]), float(point[1])] for point in points]
                        ],
                    }
                },
            }
            should.append(Q('geo_shape', **options))

        if len(should) == 1:
            # no point in producing an or query if we don't need to
            return should[0]
        else:
            # otherwise, create an or over the series of polygons
            return Bool(should=should, minimum_should_match=1)

    @staticmethod
    def create_polygon_filter(coordinates: list) -> DSLQuery:
        """
        Creates a polygon Query object. Only the first group in each polygon grouping
        will be used as elasticsearch doesn't support this type of query yet (this kind
        of query is used for vacating space inside a polygon, like a donut for example.

        If more than one group is included then they are included as an or with a
        minimum must match of 1.

        :param coordinates: a list of a list of a list of at least 3 lon/lat pairs (i.e.
            [[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]])
        :returns: a Query object
        """
        # just wrap in another list and pass to the multipolygon handler
        return BasicQuery.create_multipolygon_filter([coordinates])

    @staticmethod
    def create_geo_filter(geo_filter: Union[str, dict]) -> DSLQuery:
        """
        Creates a Query object for the geo filter specified in the geo_filter dict.

        :param geo_filter: a dict describing a geographic filter. This should be a
            GeoJSON geometry and should therefore include a type key and a coordinates
            key. The type must be one of: Point, MultiPolygon or Polygon. In the case of
            a Point, a distance key is also required which specifies the radius of the
            point in a form elasticsearch understands (for example, 10km).
        :returns: a Query object
        """
        # we support 3 GeoJSON types currently, Point, MultiPolygon and Polygon
        query_type_map = {
            'Point': (BasicQuery.create_point_filter, {'distance', 'coordinates'}),
            'MultiPolygon': (BasicQuery.create_multipolygon_filter, {'coordinates'}),
            'Polygon': (BasicQuery.create_polygon_filter, {'coordinates'}),
        }

        try:
            # if it hasn't been parsed, parse the geo_filter as JSON
            if not isinstance(geo_filter, dict):
                geo_filter = json.loads(geo_filter)
            # fetch the function which will build the query into the search object
            create_function, required_params = query_type_map[geo_filter['type']]
        except (TypeError, ValueError):
            raise toolkit.ValidationError(
                'Invalid geo filter information, must be JSON'
            )
        except KeyError:
            raise toolkit.ValidationError(
                'Invalid query type, must be point, box or polygon'
            )

        try:
            # try and pull out the required parameters for each type
            parameters = {param: geo_filter[param] for param in required_params}
        except KeyError:
            required = ', '.join(required_params)
            raise toolkit.ValidationError(
                f'Missing parameters, must include {required}'
            )

        # call the function which will update the search object to include the geo
        # filters and then return the resulting search object
        return create_function(**parameters)
