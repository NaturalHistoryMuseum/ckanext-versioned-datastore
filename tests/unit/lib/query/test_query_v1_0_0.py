# !/usr/bin/env python
# encoding: utf-8
import io
import json
import os

import jsonschema
import pytest
from splitgill.indexing.fields import DocumentField
from splitgill.search import ALL_POINTS, keyword, number, text

from ckanext.versioned_datastore.lib.query.schema import schema_base_path
from ckanext.versioned_datastore.lib.query.schemas.v1_0_0 import v1_0_0Schema


class TestV1_0_0Translator(object):
    def test_validate_examples(self):
        schema = v1_0_0Schema()
        path = os.path.join(schema_base_path, schema.version, 'examples')
        for filename in os.listdir(path):
            with io.open(os.path.join(path, filename), 'r', encoding='utf-8') as f:
                schema.validate(json.load(f))

    @staticmethod
    def compare_query_and_search(query_in, query_out):
        schema = v1_0_0Schema()
        # validate the query!
        schema.validate(query_in)
        # check that the translated version is correct compared to the expected query
        assert schema.translate(query_in).to_dict() == query_out

    def test_translate_1(self):
        query = {}
        query_out = {'match_all': {}}
        self.compare_query_and_search(query, query_out)

    def test_translate_2(self):
        query_in = {'search': 'mollusca'}
        query_out = {
            'match': {DocumentField.ALL_TEXT: {'query': 'mollusca', 'operator': 'and'}}
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_3(self):
        query_in = {
            'filters': {
                'and': [
                    {'string_equals': {'fields': ['genus'], 'value': 'helix'}},
                    {
                        'string_contains': {
                            'fields': ['higherGeography'],
                            'value': 'europe',
                        }
                    },
                ]
            }
        }
        query_out = {
            'bool': {
                'filter': [
                    {'term': {keyword('genus'): 'helix'}},
                    {
                        'match': {
                            text('higherGeography'): {
                                'query': 'europe',
                                'operator': 'and',
                            }
                        }
                    },
                ]
            }
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_4(self):
        query_in = {
            'search': 'italy',
            'filters': {
                'and': [
                    {'string_equals': {'fields': ['genus'], 'value': 'helix'}},
                    {
                        'string_contains': {
                            'fields': ['higherGeography'],
                            'value': 'europe',
                        }
                    },
                ]
            },
        }
        query_out = {
            'bool': {
                'filter': [
                    {'term': {keyword('genus'): 'helix'}},
                    {
                        'match': {
                            text('higherGeography'): {
                                'query': 'europe',
                                'operator': 'and',
                            }
                        }
                    },
                ],
                'must': [
                    {
                        'match': {
                            DocumentField.ALL_TEXT: {
                                'query': 'italy',
                                'operator': 'and',
                            }
                        }
                    }
                ],
            }
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_5(self):
        query_in = {
            'filters': {
                'and': [
                    {'string_equals': {'fields': ['genus'], 'value': 'helix'}},
                    {
                        'or': [
                            {
                                'string_contains': {
                                    'fields': ['higherGeography'],
                                    'value': 'italy',
                                }
                            },
                            {
                                'string_contains': {
                                    'fields': ['higherGeography'],
                                    'value': 'spain',
                                }
                            },
                            {
                                'string_contains': {
                                    'fields': ['higherGeography'],
                                    'value': 'portugal',
                                }
                            },
                        ]
                    },
                ]
            }
        }
        query_out = {
            'bool': {
                'filter': [
                    {'term': {keyword('genus'): 'helix'}},
                    {
                        'bool': {
                            'minimum_should_match': 1,
                            'should': [
                                {
                                    'match': {
                                        text('higherGeography'): {
                                            'query': 'italy',
                                            'operator': 'and',
                                        }
                                    }
                                },
                                {
                                    'match': {
                                        text('higherGeography'): {
                                            'query': 'spain',
                                            'operator': 'and',
                                        }
                                    }
                                },
                                {
                                    'match': {
                                        text('higherGeography'): {
                                            'query': 'portugal',
                                            'operator': 'and',
                                        }
                                    }
                                },
                            ],
                        }
                    },
                ]
            }
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_6(self):
        query_in = {
            'filters': {
                'and': [
                    {'string_equals': {'fields': ['genus'], 'value': 'helix'}},
                    {
                        'number_range': {
                            'fields': ['year'],
                            'less_than': 2010,
                            'less_than_inclusive': True,
                            'greater_than': 2000,
                            'greater_than_inclusive': True,
                        }
                    },
                    {
                        'or': [
                            {
                                'string_contains': {
                                    'fields': ['higherGeography'],
                                    'value': 'italy',
                                }
                            },
                            {
                                'string_contains': {
                                    'fields': ['higherGeography'],
                                    'value': 'spain',
                                }
                            },
                            {
                                'string_contains': {
                                    'fields': ['higherGeography'],
                                    'value': 'portugal',
                                }
                            },
                        ]
                    },
                ]
            }
        }
        query_out = {
            'bool': {
                'filter': [
                    {'term': {keyword('genus'): 'helix'}},
                    {'range': {number('year'): {'gte': 2000, 'lte': 2010}}},
                    {
                        'bool': {
                            'minimum_should_match': 1,
                            'should': [
                                {
                                    'match': {
                                        text('higherGeography'): {
                                            'query': 'italy',
                                            'operator': 'and',
                                        }
                                    }
                                },
                                {
                                    'match': {
                                        text('higherGeography'): {
                                            'query': 'spain',
                                            'operator': 'and',
                                        }
                                    }
                                },
                                {
                                    'match': {
                                        text('higherGeography'): {
                                            'query': 'portugal',
                                            'operator': 'and',
                                        }
                                    }
                                },
                            ],
                        }
                    },
                ]
            }
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_7(self):
        query_in = {
            'filters': {
                'and': [
                    {'string_equals': {'fields': ['genus'], 'value': 'helix'}},
                    {
                        'geo_point': {
                            'latitude': 51.4712,
                            'longitude': -0.9421,
                            'radius': 10,
                            'radius_unit': 'mi',
                        }
                    },
                ]
            }
        }
        query_out = {
            'bool': {
                'filter': [
                    {'term': {keyword('genus'): 'helix'}},
                    {
                        'geo_distance': {
                            DocumentField.ALL_POINTS: {
                                'lat': 51.4712,
                                'lon': -0.9421,
                            },
                            'distance': '10mi',
                        }
                    },
                ]
            }
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_8(self):
        query_in = {'filters': {'and': [{'exists': {'fields': ['associatedMedia']}}]}}
        query_out = {'exists': {'field': 'data.associatedMedia'}}
        self.compare_query_and_search(query_in, query_out)

    def test_translate_9(self):
        query_in = {'filters': {'and': [{'exists': {'geo_field': True}}]}}
        query_out = {'exists': {'field': ALL_POINTS}}
        self.compare_query_and_search(query_in, query_out)

    def test_translate_10(self):
        country = 'Curaçao'
        multipolygon = v1_0_0Schema().geojson['country'][country]
        query_in = {'filters': {'and': [{'geo_named_area': {'country': country}}]}}
        query_out = {
            'geo_polygon': {
                ALL_POINTS: {
                    'points': [
                        {'lat': point[1], 'lon': point[0]}
                        for point in multipolygon[0][0]
                    ]
                }
            }
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_11(self):
        a_square = [
            [102.0, 2.0],
            [103.0, 2.0],
            [103.0, 3.0],
            [102.0, 3.0],
            [102.0, 2.0],
        ]
        another_square = [
            [100.0, 0.0],
            [101.0, 0.0],
            [101.0, 1.0],
            [100.0, 1.0],
            [100.0, 0.0],
        ]
        a_hole = [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]

        def to_points(points):
            return [{'lat': point[1], 'lon': point[0]} for point in points]

        query_in = {
            'filters': {
                'and': [
                    {
                        'geo_custom_area': [
                            # just a square
                            [a_square],
                            # a square with a square hole in it
                            [another_square, a_hole],
                        ]
                    }
                ]
            }
        }
        query_out = {
            'bool': {
                'minimum_should_match': 1,
                'should': [
                    {'geo_polygon': {ALL_POINTS: {'points': to_points(a_square)}}},
                    {
                        'bool': {
                            'filter': [
                                {
                                    'geo_polygon': {
                                        ALL_POINTS: {
                                            'points': to_points(another_square)
                                        }
                                    }
                                }
                            ],
                            'must_not': [
                                {
                                    'geo_polygon': {
                                        ALL_POINTS: {'points': to_points(a_hole)}
                                    }
                                }
                            ],
                        }
                    },
                ],
            }
        }
        self.compare_query_and_search(query_in, query_out)

    def test_translate_12(self):
        query_in = {
            'filters': {
                'not': [{'string_equals': {'fields': ['genus'], 'value': 'helix'}}]
            }
        }
        query_out = {'bool': {'must_not': [{'term': {keyword('genus'): 'helix'}}]}}
        self.compare_query_and_search(query_in, query_out)

    def test_translate_ignores_additional_properties_in_filters(self):
        schema = v1_0_0Schema()

        nope = {
            'filters': {
                'something_else': {},
                'not': [{'string_equals': {'fields': ['genus'], 'value': 'helix'}}],
            }
        }
        with pytest.raises(jsonschema.ValidationError):
            schema.validate(nope)

    def test_translate_ignores_additional_properties_in_geo_named_area(self):
        schema = v1_0_0Schema()

        nope = {
            'filters': {
                'and': [
                    {
                        'geo_named_area': {
                            'country': 'Belgium',
                            'something_else': False,
                        }
                    }
                ]
            }
        }
        with pytest.raises(jsonschema.ValidationError):
            schema.validate(nope)
