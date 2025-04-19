import json
from unittest.mock import MagicMock

import pytest
from ckan.plugins import toolkit
from elasticsearch_dsl.query import EMPTY_QUERY, Bool, GeoShape
from splitgill.search import ALL_POINTS, ALL_TEXT, keyword, text

from ckanext.versioned_datastore.lib.query.search.query import BasicQuery


class TestAddPointFilter:
    def test_simple(self):
        distance = '4km'
        coordinates = [4.3, 1.2]

        query = BasicQuery.create_point_filter(distance, coordinates)

        assert query.to_dict() == {
            'geo_distance': {
                'distance': '4km',
                ALL_POINTS: {
                    'lat': coordinates[1],
                    'lon': coordinates[0],
                },
            }
        }

    def test_float_conversion(self):
        distance = '4km'
        coordinates = ['4.53423', 100]

        query = BasicQuery.create_point_filter(distance, coordinates)

        assert query.to_dict() == {
            'geo_distance': {
                'distance': '4km',
                ALL_POINTS: {
                    'lat': 100.0,
                    'lon': 4.53423,
                },
            }
        }


class TestAddMultiPolygonFilter:
    def test_single(self):
        coordinates = [[[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]]]

        query = BasicQuery.create_multipolygon_filter(coordinates)

        assert query == GeoShape(
            **{
                ALL_POINTS: {
                    'shape': {
                        'type': 'Polygon',
                        'coordinates': [
                            [[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]
                        ],
                    }
                }
            }
        )

    def test_multi(self):
        coordinates = [
            [[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]],
            [[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]],
        ]

        query = BasicQuery.create_multipolygon_filter(coordinates)

        expected = GeoShape(
            **{
                ALL_POINTS: {
                    'shape': {
                        'type': 'Polygon',
                        'coordinates': [
                            [
                                [-16, 44],
                                [-13.1, 34.8],
                                [15.99, 35],
                                [5, 49],
                            ]
                        ],
                    }
                }
            }
        )
        assert query == Bool(should=[expected, expected], minimum_should_match=1)

    def test_validation_error(self):
        # only two points!
        coordinates = [[[[-16, 44], [-13.1, 34.8]]]]

        with pytest.raises(toolkit.ValidationError):
            BasicQuery.create_multipolygon_filter(coordinates)

    def test_float_conversion(self):
        coordinates = [
            [[['-16', '44'], ['-13.1', '34.8'], ['15.99', '35'], ['5', '49']]]
        ]

        query = BasicQuery.create_multipolygon_filter(coordinates)

        assert query == GeoShape(
            **{
                ALL_POINTS: {
                    'shape': {
                        'type': 'Polygon',
                        'coordinates': [
                            [
                                [-16, 44],
                                [-13.1, 34.8],
                                [15.99, 35],
                                [5, 49],
                            ]
                        ],
                    },
                }
            }
        )


class TestAddPolygonFilter:
    def test_simple(self):
        coordinates = [[['-16', '44'], ['-13.1', '34.8'], ['15.99', '35'], ['5', '49']]]

        query = BasicQuery.create_polygon_filter(coordinates)

        assert query == GeoShape(
            **{
                ALL_POINTS: {
                    'shape': {
                        'type': 'Polygon',
                        'coordinates': [
                            [
                                [-16, 44],
                                [-13.1, 34.8],
                                [15.99, 35],
                                [5, 49],
                            ]
                        ],
                    }
                }
            }
        )


class TestAddGeoSearch:
    def test_valid_point_dict(self):
        geo_filter = {
            'type': 'Point',
            'distance': MagicMock(),
            'coordinates': MagicMock(),
        }
        query = BasicQuery.create_geo_filter(geo_filter)
        assert query == BasicQuery.create_point_filter(
            geo_filter['distance'],
            geo_filter['coordinates'],
        )

    def test_valid_point_string(self):
        geo_filter = json.dumps(
            {
                'type': 'Point',
                'distance': '10km',
                'coordinates': [40, 30],
            }
        )
        query = BasicQuery.create_geo_filter(geo_filter)
        assert query == BasicQuery.create_point_filter(
            '10km',
            [40, 30],
        )

    def test_invalid_point(self):
        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                'type': 'Point',
            }
            BasicQuery.create_geo_filter(geo_filter)

        for param in ('distance', 'coordinates'):
            with pytest.raises(toolkit.ValidationError):
                geo_filter = {
                    'type': 'Point',
                    param: MagicMock(),
                }
                BasicQuery.create_geo_filter(geo_filter)

    def test_valid_multipolygon_dict(self):
        geo_filter = {
            'type': 'MultiPolygon',
            'coordinates': [[[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]]],
        }
        query = BasicQuery.create_geo_filter(geo_filter)
        assert query == BasicQuery.create_multipolygon_filter(geo_filter['coordinates'])

    def test_valid_multipolygon_string(self):
        coords = [[[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]]]
        geo_filter = json.dumps({'type': 'MultiPolygon', 'coordinates': coords})
        query = BasicQuery.create_geo_filter(geo_filter)
        assert query == BasicQuery.create_multipolygon_filter(coords)

    def test_invalid_multipolygon(self):
        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                'type': 'MultiPolygon',
            }
            BasicQuery.create_geo_filter(geo_filter)

    def test_valid_polygon_dict(self):
        geo_filter = {
            'type': 'Polygon',
            'coordinates': [[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]],
        }
        query = BasicQuery.create_geo_filter(geo_filter)
        assert query == BasicQuery.create_polygon_filter(geo_filter['coordinates'])

    def test_valid_polygon_string(self):
        geo_filter = {
            'type': 'Polygon',
            'coordinates': [[[-16, 44], [-13.1, 34.8], [15.99, 35], [5, 49]]],
        }
        query = BasicQuery.create_geo_filter(json.dumps(geo_filter))
        assert query == BasicQuery.create_polygon_filter(geo_filter['coordinates'])

    def test_invalid_polygon(self):
        with pytest.raises(toolkit.ValidationError):
            geo_filter = {
                'type': 'Polygon',
            }
            BasicQuery.create_geo_filter(geo_filter)

    def test_invalid_type(self):
        with pytest.raises(toolkit.ValidationError):
            # a type we don't support
            BasicQuery.create_geo_filter({'type': 'not a type we support'})

        with pytest.raises(toolkit.ValidationError):
            # no type
            BasicQuery.create_geo_filter({})

    def test_invalid_json(self):
        # here's a valid geo_filter value
        geo_filter = {
            'type': 'Polygon',
            # can't use magic mocks here because they need to be JSON serialised
            'coordinates': 'coord mock',
        }
        # but we're gonna add the word beans at the end to make it invalid
        geo_filter = json.dumps(geo_filter) + 'beans'

        with pytest.raises(toolkit.ValidationError):
            BasicQuery.create_geo_filter(geo_filter)


class TestToDSL:
    def test_empty_q(self):
        assert BasicQuery('test').to_dsl() == EMPTY_QUERY
        assert BasicQuery('test', q='').to_dsl() == EMPTY_QUERY
        assert BasicQuery('test', q=None).to_dsl() == EMPTY_QUERY
        assert BasicQuery('test', q={}).to_dsl() == EMPTY_QUERY

    def _run_test(self, data_dict: dict, expected: dict):
        assert BasicQuery('test', **data_dict).to_dsl().to_dict() == expected

    def test_q_simple_text(self):
        self._run_test(
            {'q': 'banana'},
            {
                'match': {
                    ALL_TEXT: {
                        'query': 'banana',
                        'operator': 'and',
                    }
                }
            },
        )
        self._run_test(
            {'q': 'a multi-word example'},
            {
                'match': {
                    ALL_TEXT: {
                        'query': 'a multi-word example',
                        'operator': 'and',
                    }
                }
            },
        )

    def test_q_dicts(self):
        self._run_test(
            {'q': {'': 'banana'}},
            {
                'match': {
                    ALL_TEXT: {
                        'query': 'banana',
                        'operator': 'and',
                    }
                }
            },
        )
        self._run_test(
            {'q': {'field1': 'banana'}},
            {
                'match': {
                    text('field1'): {
                        'query': 'banana',
                        'operator': 'and',
                    }
                }
            },
        )
        self._run_test(
            {'q': {'field1': 'banana', 'field2': 'lemons'}},
            {
                'bool': {
                    'must': [
                        {
                            'match': {
                                text('field1'): {
                                    'query': 'banana',
                                    'operator': 'and',
                                },
                            }
                        },
                        {
                            'match': {
                                text('field2'): {
                                    'query': 'lemons',
                                    'operator': 'and',
                                },
                            }
                        },
                    ]
                }
            },
        )

    def test_q_string_and_unicode(self):
        self._run_test(
            {'q': 'a string'},
            {
                'match': {
                    ALL_TEXT: {
                        'query': 'a string',
                        'operator': 'and',
                    }
                }
            },
        )
        self._run_test(
            {'q': 'a unicode string'},
            {
                'match': {
                    ALL_TEXT: {
                        'query': 'a unicode string',
                        'operator': 'and',
                    }
                }
            },
        )

    def test_q_non_string(self):
        self._run_test(
            {'q': 4},
            {
                'match': {
                    ALL_TEXT: {
                        'query': '4',
                        'operator': 'and',
                    }
                }
            },
        )
        self._run_test(
            {'q': 4.31932},
            {
                'match': {
                    ALL_TEXT: {
                        'query': '4.31932',
                        'operator': 'and',
                    }
                }
            },
        )

    def test_filters_empties(self):
        self._run_test({'filters': {}}, EMPTY_QUERY.to_dict())

    def test_filters_non_lists(self):
        self._run_test(
            {
                'filters': {
                    'field1': 'banana',
                }
            },
            {
                'term': {
                    keyword('field1'): 'banana',
                }
            },
        )
        self._run_test(
            {
                'filters': {
                    'field1': 'banana',
                    'field2': 'lemons',
                }
            },
            {
                'bool': {
                    'filter': [
                        {
                            'term': {
                                keyword('field1'): 'banana',
                            }
                        },
                        {
                            'term': {
                                keyword('field2'): 'lemons',
                            }
                        },
                    ]
                }
            },
        )

    def test_filters_lists(self):
        self._run_test(
            {
                'filters': {
                    'field1': ['banana'],
                }
            },
            {
                'term': {
                    keyword('field1'): 'banana',
                }
            },
        )
        self._run_test(
            {
                'filters': {
                    'field1': ['banana'],
                    'field2': ['lemons'],
                }
            },
            {
                'bool': {
                    'filter': [
                        {
                            'term': {
                                keyword('field1'): 'banana',
                            }
                        },
                        {
                            'term': {
                                keyword('field2'): 'lemons',
                            }
                        },
                    ]
                }
            },
        )
        self._run_test(
            {
                'filters': {
                    'field1': ['banana', 'goat', 'funnel'],
                    'field2': ['lemons', 'chunk'],
                }
            },
            {
                'bool': {
                    'filter': [
                        {
                            'term': {
                                keyword('field1'): 'banana',
                            }
                        },
                        {
                            'term': {
                                keyword('field1'): 'goat',
                            }
                        },
                        {
                            'term': {
                                keyword('field1'): 'funnel',
                            }
                        },
                        {
                            'term': {
                                keyword('field2'): 'lemons',
                            }
                        },
                        {
                            'term': {
                                keyword('field2'): 'chunk',
                            }
                        },
                    ]
                }
            },
        )

    def test_filters_mix(self):
        self._run_test(
            {
                'filters': {
                    'field1': 'banana',
                    'field2': ['lemons', 'blarp'],
                }
            },
            {
                'bool': {
                    'filter': [
                        {
                            'term': {
                                keyword('field1'): 'banana',
                            }
                        },
                        {
                            'term': {
                                keyword('field2'): 'lemons',
                            }
                        },
                        {
                            'term': {
                                keyword('field2'): 'blarp',
                            }
                        },
                    ]
                }
            },
        )
