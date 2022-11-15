import json

import pytest
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.basic_query.geo import (
    add_point_filter,
    FIELD,
    add_multipolygon_filter,
    add_polygon_filter,
    add_geo_search,
)
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
        assert search.filter.call_args == call(
            'geo_distance',
            **{
                'distance': distance,
                FIELD: {
                    'lat': coordinates[1],
                    'lon': coordinates[0],
                },
            }
        )

    def test_float_conversion(self):
        search = MagicMock(filter=MagicMock())
        distance = MagicMock()
        coordinates = ('4.53423', 100)

        returned_search = add_point_filter(search, distance, coordinates)

        assert returned_search == search.filter.return_value
        assert search.filter.call_count == 1
        assert search.filter.call_args == call(
            'geo_distance',
            **{
                'distance': distance,
                FIELD: {
                    'lat': float(coordinates[1]),
                    'lon': float(coordinates[0]),
                },
            }
        )


class TestAddMultiPolygonFilter(object):
    def test_simple(self):
        search = MagicMock(filter=MagicMock())
        coordinates = [[[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]]]

        returned_search = add_multipolygon_filter(search, coordinates)

        assert returned_search == search.filter.return_value
        assert search.filter.call_count == 1

        filters = [
            GeoPolygon(
                **{
                    FIELD: {
                        'points': [
                            {
                                'lat': 44.0,
                                'lon': -16.0,
                            },
                            {
                                'lat': 34.8,
                                'lon': -13.1,
                            },
                            {
                                'lat': 35.0,
                                'lon': 15.99,
                            },
                            {
                                'lat': 49.0,
                                'lon': 5.0,
                            },
                        ]
                    }
                }
            )
        ]
        assert search.filter.call_args == call(
            Bool(should=filters, minimum_should_match=1)
        )

    def test_validation_error(self):
        search = MagicMock(filter=MagicMock())
        # only two points!
        coordinates = [[[[-16, 44], [-13.1, 34.8]]]]

        with pytest.raises(toolkit.ValidationError):
            add_multipolygon_filter(search, coordinates)

    def test_float_conversion(self):
        search = MagicMock(filter=MagicMock())
        coordinates = [
            [[['-16', '44'], ['-13.1', '34.8'], ['15.99', '35'], ['5', '49']]]
        ]

        returned_search = add_multipolygon_filter(search, coordinates)

        assert returned_search == search.filter.return_value
        assert search.filter.call_count == 1

        filters = [
            GeoPolygon(
                **{
                    FIELD: {
                        'points': [
                            {
                                'lat': 44.0,
                                'lon': -16.0,
                            },
                            {
                                'lat': 34.8,
                                'lon': -13.1,
                            },
                            {
                                'lat': 35.0,
                                'lon': 15.99,
                            },
                            {
                                'lat': 49.0,
                                'lon': 5.0,
                            },
                        ]
                    }
                }
            )
        ]
        assert search.filter.call_args == call(
            Bool(should=filters, minimum_should_match=1)
        )


class TestAddPolygonFilter(object):
    def test_pass_off(self):
        # add_polygon_filter just uses add_multipolygon_filter which we already have a test for so
        # we can just test that it is called
        search = MagicMock()
        coordinates = [[['-16', '44'], ['-13.1', '34.8'], ['15.99', '35'], ['5', '49']]]
        mock_add_multipolygon_filter = MagicMock()

        with patch(
            'ckanext.versioned_datastore.lib.basic_query.geo.add_multipolygon_filter',
            mock_add_multipolygon_filter,
        ):
            add_polygon_filter(search, coordinates)

        assert mock_add_multipolygon_filter.call_args == call(search, [coordinates])


class TestAddGeoSearch(object):
    def test_valid_point_dict(self):
        search = MagicMock()
        geo_filter = {
            'type': 'Point',
            'distance': MagicMock(),
            'coordinates': MagicMock(),
        }

        add_mock = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.geo.add_point_filter', add_mock
        ):
            add_geo_search(search, geo_filter)

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(
            search,
            distance=geo_filter['distance'],
            coordinates=geo_filter['coordinates'],
        )

    def test_valid_point_string(self):
        search = MagicMock()
        geo_filter = {
            'type': 'Point',
            # can't use magic mocks here because they need to be JSON serialised
            'distance': 'distance mock',
            'coordinates': 'coord mock',
        }

        add_mock = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.geo.add_point_filter', add_mock
        ):
            add_geo_search(search, json.dumps(geo_filter))

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(
            search,
            distance=geo_filter['distance'],
            coordinates=geo_filter['coordinates'],
        )

    def test_invalid_point(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                'type': 'Point',
            }
            add_geo_search(search, geo_filter)

        for param in ('distance', 'coordinates'):
            with pytest.raises(toolkit.ValidationError):
                geo_filter = {
                    'type': 'Point',
                    param: MagicMock(),
                }
                add_geo_search(search, geo_filter)

    def test_valid_multipolygon_dict(self):
        search = MagicMock()
        geo_filter = {
            'type': 'MultiPolygon',
            'coordinates': MagicMock(),
        }

        add_mock = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.geo.add_multipolygon_filter',
            add_mock,
        ):
            add_geo_search(search, geo_filter)

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter['coordinates'])

    def test_valid_multipolygon_string(self):
        search = MagicMock()
        geo_filter = {
            'type': 'MultiPolygon',
            # can't use magic mocks here because they need to be JSON serialised
            'coordinates': 'coord mock',
        }

        add_mock = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.geo.add_multipolygon_filter',
            add_mock,
        ):
            add_geo_search(search, json.dumps(geo_filter))

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter['coordinates'])

    def test_invalid_multipolygon(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                'type': 'MultiPolygon',
            }
            add_geo_search(search, geo_filter)

    def test_valid_polygon_dict(self):
        search = MagicMock()
        geo_filter = {
            'type': 'Polygon',
            'coordinates': MagicMock(),
        }

        add_mock = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.geo.add_polygon_filter',
            add_mock,
        ):
            add_geo_search(search, geo_filter)

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter['coordinates'])

    def test_valid_polygon_string(self):
        search = MagicMock()
        geo_filter = {
            'type': 'Polygon',
            # can't use magic mocks here because they need to be JSON serialised
            'coordinates': 'coord mock',
        }

        add_mock = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.geo.add_polygon_filter',
            add_mock,
        ):
            add_geo_search(search, json.dumps(geo_filter))

        assert add_mock.call_count == 1
        assert add_mock.call_args == call(search, coordinates=geo_filter['coordinates'])

    def test_invalid_polygon(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                'type': 'Polygon',
            }
            add_geo_search(search, geo_filter)

    def test_invalid_type(self):
        search = MagicMock()

        with pytest.raises(toolkit.ValidationError):
            # a type we don't support
            add_geo_search(search, {'type': 'not a type we support'})

        with pytest.raises(toolkit.ValidationError):
            # no type
            add_geo_search(search, {})

    def test_invalid_json(self):
        search = MagicMock()
        # here's a valid geo_filter value
        geo_filter = {
            'type': 'Polygon',
            # can't use magic mocks here because they need to be JSON serialised
            'coordinates': 'coord mock',
        }
        # but we're gonna add the word beans at the end to make it invalid
        geo_filter = json.dumps(geo_filter) + 'beans'

        with pytest.raises(toolkit.ValidationError):
            add_geo_search(search, geo_filter)
