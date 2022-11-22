import json
from ckan.plugins import toolkit
from elasticsearch_dsl import Q

# the geo field is always meta.geo
FIELD = 'meta.geo'


def add_point_filter(search, distance, coordinates):
    """
    Adds a point filter query to the search object and returns a new search object.

    :param search: the current elasticsearch DSL object
    :param distance: the radius of the circle centred on the specified location within which records
                     must lie to be matched. This can specified in any form that elasticsearch
                     accepts for distances (see their doc, but essentially values like 10km etc).
    :param coordinates: the point to centre the radius on, specified as a lon/lat pair in a list
                        (i.e. [40.2, -20]).
    :return: a search object
    """
    options = {
        'distance': distance,
        FIELD: {
            'lat': float(coordinates[1]),
            'lon': float(coordinates[0]),
        },
    }
    return search.filter('geo_distance', **options)


def add_multipolygon_filter(search, coordinates):
    """
    Adds a multipolygon filter query to the search object and returns a new search
    object. Only the first group in each polygon grouping will be used as elasticsearch
    doesn't support this type of query yet (this kind of query is used for vacating
    space inside a polygon, like a donut for example.

    If more than one group is included then they are included as an or with a minimum must match of
    1.

    :param search: the current elasticsearch DSL object
    :param coordinates: a list of a list of a list of a list of at least 3 lon/lat pairs (i.e.
                        [[[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]]])
    :return: a search object
    """
    filters = []
    for group in coordinates:
        points = group[0]
        if len(points) < 3:
            raise toolkit.ValidationError(
                'Not enough points in the polygon, must be 3 or more'
            )

        options = {
            FIELD: {
                # format the polygon point data appropriately
                'points': [
                    {'lat': float(point[1]), 'lon': float(point[0])} for point in points
                ],
            },
        }
        filters.append(Q('geo_polygon', **options))
    # add the filter to the search as an or
    return search.filter(Q('bool', should=filters, minimum_should_match=1))


def add_polygon_filter(search, coordinates):
    """
    Adds a polygon filter query to the search object and returns a new search object.
    Only the first group in each polygon grouping will be used as elasticsearch doesn't
    support this type of query yet (this kind of query is used for vacating space inside
    a polygon, like a donut for example.

    If more than one group is included then they are included as an or with a minimum must match of
    1.

    :param search: the current elasticsearch DSL object
    :param coordinates: a list of a list of a list of at least 3 lon/lat pairs (i.e. [[[-16, 44],
                        [-13.1, 34.8], [15.99, 35], [5, 49]]])
    :return: a search object
    """
    # just wrap in another list and pass to the multipolygon handler
    return add_multipolygon_filter(search, [coordinates])


def add_geo_search(search, geo_filter):
    """
    Updates the given search DSL object with the geo filter specified in the geo_filter
    dict.

    :param search: the current elasticsearch DSL object
    :param geo_filter: a dict describing a geographic filter. This should be a GeoJSON geometry and
                       should therefore include a type key and a coordinates key. The type must be
                       one of: Point, MultiPolygon or Polygon. In the case of a Point, a distance
                       key is also required which specifies the radius of the point in a form
                       elasticsearch understands (for example, 10km).
    :return: a search DSL object
    """
    # we support 3 GeoJSON types currently, Point, MultiPolygon and Polygon
    query_type_map = {
        'Point': (add_point_filter, {'distance', 'coordinates'}),
        'MultiPolygon': (add_multipolygon_filter, {'coordinates'}),
        'Polygon': (add_polygon_filter, {'coordinates'}),
    }

    try:
        # if it hasn't been parsed, parse the geo_filter as JSON
        if not isinstance(geo_filter, dict):
            geo_filter = json.loads(geo_filter)
        # fetch the function which will build the query into the search object
        add_function, required_params = query_type_map[geo_filter['type']]
    except (TypeError, ValueError):
        raise toolkit.ValidationError('Invalid geo filter information, must be JSON')
    except KeyError:
        raise toolkit.ValidationError(
            'Invalid query type, must be point, box or polygon'
        )

    try:
        # try and pull out the required parameters for each type
        parameters = {param: geo_filter[param] for param in required_params}
    except KeyError:
        required = ', '.join(required_params)
        raise toolkit.ValidationError(f'Missing parameters, must include {required}')

    # call the function which will update the search object to include the geo filters and then
    # return the resulting search object
    return add_function(search, **parameters)
