import json

import pytest
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.basic_query.geo import add_point_filter, FIELD, \
    add_multipolygon_filter, add_polygon_filter, add_geo_search
from elasticsearch_dsl.query import GeoPolygon, Bool
from mock import MagicMock, call, patch


class TestAddPointFilter(object):

    def test_simple(self):
        search = MagicMock(filter=MagicMock())
        distance = MagicMock()
        coordinates = (4.3, 1.2)

        returned_search = add_point_filter(search, distance, coordinates)

        assert returned_search == search.filter.return_value
        assert search.filter.call_count == 1
        assert search.filter.call_args == call(u'geo_distance',
                                               **{
                                                   u'distance': distance,
                                                   FIELD: {
                                                       u'lat': coordinates[1],
                                                       u'lon': coordinates[0],
                                                   }
                                               })

    def test_float_conversion(self):
        search = MagicMock(filter=MagicMock())
        distance = MagicMock()
        coordinates = (u'4.53423', 100)

        returned_search = add_point_filter(search, distance, coordinates)

        assert returned_search == search.filter.return_value
        assert search.filter.call_count == 1
        assert search.filter.call_args == call(u'geo_distance',
                                               **{
                                                   u'distance': distance,
                                                   FIELD: {
                                                       u'lat': float(coordinates[1]),
                                                       u'lon': float(coordinates[0]),
                                                   }
                                               })


class TestAddMultiPolygonFilter(object):

    def test_simple(self):
        search = MagicMock(filter=MagicMock())
        coordinates = [[[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]]]

        returned_search = add_multipolygon_filter(search, coordinates)

        assert returned_search == search.filter.return_value
        assert search.filter.call_count == 1

        filters = [
            GeoPolygon(**{
                FIELD: {
                    u'points': [
                        {
                            u'lat': 44.0,
                            u'lon': -16.0,
                        },
                        {
                            u'lat': 34.8,
                            u'lon': -13.1,
                        },
                        {
                            u'lat': 35.0,
                            u'lon': 15.99,
                        },
                        {
                            u'lat': 49.0,
                            u'lon': 5.0,
                        },
                    ]
                }
            })
        ]
        assert search.filter.call_args == call(Bool(should=filters, minimum_should_match=1))

    def test_validation_error(self):
        search = MagicMock(filter=MagicMock())
        # only two points!
        coordinates = [[[[-16, 44], [-13.1, 34.8]]]]

        with pytest.raises(toolkit.ValidationError):
            add_multipolygon_filter(search, coordinates)

    def test_float_conversion(self):
        search = MagicMock(filter=MagicMock())
        coordinates = [[[[u'-16', u'44'], [u'-13.1', u'34.8'], [u'15.99', u'35'], [u'5', u'49']]]]

        returned_search = add_multipolygon_filter(search, coordinates)

        assert returned_search == search.filter.return_value
        assert search.filter.call_count == 1

        filters = [
            GeoPolygon(**{
                FIELD: {
                    u'points': [
                        {
                            u'lat': 44.0,
                            u'lon': -16.0,
                        },
                        {
                            u'lat': 34.8,
                            u'lon': -13.1,
                        },
                        {
                            u'lat': 35.0,
                            u'lon': 15.99,
                        },
                        {
                            u'lat': 49.0,
                            u'lon': 5.0,
                        },
                    ]
                }
            })
        ]
        assert search.filter.call_args == call(Bool(should=filters, minimum_should_match=1))


class TestAddPolygonFilter(object):

    def test_pass_off(self):
        # add_polygon_filter just uses add_multipolygon_filter which we already have a test for so
        # we can just test that it is called
        search = MagicMock()
        coordinates = [[[u'-16', u'44'], [u'-13.1', u'34.8'], [u'15.99', u'35'], [u'5', u'49']]]
        mock_add_multipolygon_filter = MagicMock()

        with patch(u'ckanext.versioned_datastore.lib.basic_query.geo.add_multipolygon_filter',
                   mock_add_multipolygon_filter):
            add_polygon_filter(search, coordinates)

        assert mock_add_multipolygon_filter.call_args == call(search, [coordinates])


class TestAddGeoSearch(object):

    def test_valid_point_dict(self):
        search = MagicMock()
        geo_filter = {
            u'type': u'Point',
            u'distance': MagicMock(),
            u'coordinates': MagicMock(),
        }

        add_mock = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.basic_query.geo.add_point_filter', add_mock):
            add_geo_search(search, geo_filter)

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, distance=geo_filter[u'distance'],
                                          coordinates=geo_filter[u'coordinates'])

    def test_valid_point_string(self):
        search = MagicMock()
        geo_filter = {
            u'type': u'Point',
            # can't use magic mocks here because they need to be JSON serialised
            u'distance': u'distance mock',
            u'coordinates': u'coord mock',
        }

        add_mock = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.basic_query.geo.add_point_filter', add_mock):
            add_geo_search(search, json.dumps(geo_filter))

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, distance=geo_filter[u'distance'],
                                          coordinates=geo_filter[u'coordinates'])

    def test_invalid_point(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                u'type': u'Point',
            }
            add_geo_search(search, geo_filter)

        for param in (u'distance', u'coordinates'):
            with pytest.raises(toolkit.ValidationError):
                geo_filter = {
                    u'type': u'Point',
                    param: MagicMock(),
                }
                add_geo_search(search, geo_filter)

    def test_valid_multipolygon_dict(self):
        search = MagicMock()
        geo_filter = {
            u'type': u'MultiPolygon',
            u'coordinates': MagicMock(),
        }

        add_mock = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.basic_query.geo.add_multipolygon_filter',
                   add_mock):
            add_geo_search(search, geo_filter)

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter[u'coordinates'])

    def test_valid_multipolygon_string(self):
        search = MagicMock()
        geo_filter = {
            u'type': u'MultiPolygon',
            # can't use magic mocks here because they need to be JSON serialised
            u'coordinates': u'coord mock',
        }

        add_mock = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.basic_query.geo.add_multipolygon_filter',
                   add_mock):
            add_geo_search(search, json.dumps(geo_filter))

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter[u'coordinates'])

    def test_invalid_multipolygon(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                u'type': u'MultiPolygon',
            }
            add_geo_search(search, geo_filter)

    def test_valid_polygon_dict(self):
        search = MagicMock()
        geo_filter = {
            u'type': u'Polygon',
            u'coordinates': MagicMock(),
        }

        add_mock = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.basic_query.geo.add_polygon_filter', add_mock):
            add_geo_search(search, geo_filter)

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter[u'coordinates'])

    def test_valid_polygon_string(self):
        search = MagicMock()
        geo_filter = {
            u'type': u'Polygon',
            # can't use magic mocks here because they need to be JSON serialised
            u'coordinates': u'coord mock',
        }

        add_mock = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.basic_query.geo.add_polygon_filter', add_mock):
            add_geo_search(search, json.dumps(geo_filter))

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter[u'coordinates'])

    def test_invalid_polygon(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                u'type': u'Polygon',
            }
            add_geo_search(search, geo_filter)

    def test_invalid_type(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            # a type we don't support
            add_geo_search(search, {u'type': u'not a type we support'})

        with pytest.raises(toolkit.ValidationError):
            # no type
            add_geo_search(search, {})

    def test_invalid_json(self):
        search = MagicMock()
        # here's a valid geo_filter value
        geo_filter = {
            u'type': u'Polygon',
            # can't use magic mocks here because they need to be JSON serialised
            u'coordinates': u'coord mock',
        }
        # but we're gonna add the word beans at the end to make it invalid
        geo_filter = json.dumps(geo_filter) + u'beans'

        with pytest.raises(toolkit.ValidationError):
            add_geo_search(search, geo_filter)
