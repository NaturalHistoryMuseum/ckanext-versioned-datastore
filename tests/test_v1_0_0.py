# !/usr/bin/env python
# encoding: utf-8
import io
import json

import jsonschema
import os

import pytest
from ckanext.versioned_datastore.lib.query.schema import schema_base_path
from ckanext.versioned_datastore.lib.query.v1_0_0 import v1_0_0Schema


class TestV1_0_0Translator(object):
    def test_validate_examples(self):
        schema = v1_0_0Schema()
        path = os.path.join(schema_base_path, schema.version, 'examples')
        for filename in os.listdir(path):
            with io.open(os.path.join(path, filename), 'r', encoding='utf-8') as f:
                schema.validate(json.load(f))

    @staticmethod
    def compare_query_and_search(query, search_dict):
        schema = v1_0_0Schema()
        # validate the query!
        schema.validate(query)
        # check that the translated version is correct compared to the expected search dict
        assert schema.translate(query).to_dict() == search_dict

    def test_translate_1(self):
        query = {}
        search_dict = {}
        self.compare_query_and_search(query, search_dict)

    def test_translate_2(self):
        query = {'search': 'mollusca'}
        search_dict = {
            'query': {'match': {'meta.all': {'query': 'mollusca', 'operator': 'and'}}}
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_3(self):
        query = {
            u"filters": {
                u"and": [
                    {u"string_equals": {u"fields": [u"genus"], u"value": u"helix"}},
                    {
                        u"string_contains": {
                            u"fields": [u"higherGeography"],
                            u"value": u"europe",
                        }
                    },
                ]
            }
        }
        search_dict = {
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'data.genus': 'helix'}},
                        {
                            'match': {
                                'data.higherGeography.full': {
                                    'query': 'europe',
                                    'operator': 'and',
                                }
                            }
                        },
                    ]
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_4(self):
        query = {
            'search': 'italy',
            u"filters": {
                u"and": [
                    {u"string_equals": {u"fields": [u"genus"], u"value": u"helix"}},
                    {
                        u"string_contains": {
                            u"fields": [u"higherGeography"],
                            u"value": u"europe",
                        }
                    },
                ]
            },
        }
        search_dict = {
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'data.genus': 'helix'}},
                        {
                            'match': {
                                'data.higherGeography.full': {
                                    'query': 'europe',
                                    'operator': 'and',
                                }
                            }
                        },
                    ],
                    'must': [
                        {'match': {'meta.all': {'query': 'italy', 'operator': 'and'}}}
                    ],
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_5(self):
        query = {
            u"filters": {
                u"and": [
                    {u"string_equals": {u"fields": [u"genus"], u"value": u"helix"}},
                    {
                        u"or": [
                            {
                                u"string_contains": {
                                    u"fields": [u"higherGeography"],
                                    u"value": u"italy",
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [u"higherGeography"],
                                    u"value": u"spain",
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [u"higherGeography"],
                                    u"value": u"portugal",
                                }
                            },
                        ]
                    },
                ]
            }
        }
        search_dict = {
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'data.genus': 'helix'}},
                        {
                            'bool': {
                                'minimum_should_match': 1,
                                'should': [
                                    {
                                        'match': {
                                            'data.higherGeography.full': {
                                                'query': 'italy',
                                                'operator': 'and',
                                            }
                                        }
                                    },
                                    {
                                        'match': {
                                            'data.higherGeography.full': {
                                                'query': 'spain',
                                                'operator': 'and',
                                            }
                                        }
                                    },
                                    {
                                        'match': {
                                            'data.higherGeography.full': {
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
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_6(self):
        query = {
            u"filters": {
                u"and": [
                    {u"string_equals": {u"fields": [u"genus"], u"value": u"helix"}},
                    {
                        u"number_range": {
                            u"fields": [u"year"],
                            u"less_than": 2010,
                            u"less_than_inclusive": True,
                            u"greater_than": 2000,
                            u"greater_than_inclusive": True,
                        }
                    },
                    {
                        u"or": [
                            {
                                u"string_contains": {
                                    u"fields": [u"higherGeography"],
                                    u"value": u"italy",
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [u"higherGeography"],
                                    u"value": u"spain",
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [u"higherGeography"],
                                    u"value": u"portugal",
                                }
                            },
                        ]
                    },
                ]
            }
        }
        search_dict = {
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'data.genus': 'helix'}},
                        {'range': {'data.year.number': {'gte': 2000, 'lte': 2010}}},
                        {
                            'bool': {
                                'minimum_should_match': 1,
                                'should': [
                                    {
                                        'match': {
                                            'data.higherGeography.full': {
                                                'query': 'italy',
                                                'operator': 'and',
                                            }
                                        }
                                    },
                                    {
                                        'match': {
                                            'data.higherGeography.full': {
                                                'query': 'spain',
                                                'operator': 'and',
                                            }
                                        }
                                    },
                                    {
                                        'match': {
                                            'data.higherGeography.full': {
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
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_7(self):
        query = {
            u"filters": {
                u"and": [
                    {u"string_equals": {u"fields": [u"genus"], u"value": u"helix"}},
                    {
                        u"geo_point": {
                            u"latitude": 51.4712,
                            u"longitude": -0.9421,
                            u"radius": 10,
                            u"radius_unit": u"mi",
                        }
                    },
                ]
            }
        }
        search_dict = {
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'data.genus': 'helix'}},
                        {
                            'geo_distance': {
                                'meta.geo': {'lat': 51.4712, 'lon': -0.9421},
                                'distance': '10mi',
                            }
                        },
                    ]
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_8(self):
        query = {u"filters": {u"and": [{u"exists": {u"fields": [u"associatedMedia"]}}]}}
        search_dict = {'query': {'exists': {'field': 'data.associatedMedia'}}}
        self.compare_query_and_search(query, search_dict)

    def test_translate_9(self):
        query = {u"filters": {u"and": [{u"exists": {u"geo_field": True}}]}}
        search_dict = {'query': {'exists': {'field': 'meta.geo'}}}
        self.compare_query_and_search(query, search_dict)

    def test_translate_10(self):
        country = 'Curaçao'
        multipolygon = v1_0_0Schema().geojson['country'][country]
        query = {u"filters": {u"and": [{u"geo_named_area": {u"country": country}}]}}
        search_dict = {
            'query': {
                'geo_polygon': {
                    'meta.geo': {
                        'points': [
                            {'lat': point[1], 'lon': point[0]}
                            for point in multipolygon[0][0]
                        ]
                    }
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

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

        query = {
            u"filters": {
                u"and": [
                    {
                        u"geo_custom_area": [
                            # just a square
                            [a_square],
                            # a square with a square hole in it
                            [another_square, a_hole],
                        ]
                    }
                ]
            }
        }
        search_dict = {
            u"query": {
                u"bool": {
                    u"minimum_should_match": 1,
                    u"should": [
                        {
                            u"geo_polygon": {
                                u"meta.geo": {u"points": to_points(a_square)}
                            }
                        },
                        {
                            u"bool": {
                                u"filter": [
                                    {
                                        u"geo_polygon": {
                                            u"meta.geo": {
                                                u"points": to_points(another_square)
                                            }
                                        }
                                    }
                                ],
                                u"must_not": [
                                    {
                                        u"geo_polygon": {
                                            u"meta.geo": {u"points": to_points(a_hole)}
                                        }
                                    }
                                ],
                            }
                        },
                    ],
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_12(self):
        query = {
            u"filters": {
                u"not": [
                    {u"string_equals": {u"fields": [u"genus"], u"value": u"helix"}}
                ]
            }
        }
        search_dict = {
            'query': {'bool': {'must_not': [{'term': {'data.genus': 'helix'}}]}}
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_ignores_additional_properties_in_filters(self):
        schema = v1_0_0Schema()

        nope = {
            u"filters": {
                'something_else': {},
                u"not": [
                    {u"string_equals": {u"fields": [u"genus"], u"value": u"helix"}}
                ],
            }
        }
        with pytest.raises(jsonschema.ValidationError):
            schema.validate(nope)

    def test_translate_ignores_additional_properties_in_geo_named_area(self):
        schema = v1_0_0Schema()

        nope = {
            u"filters": {
                u"and": [
                    {
                        u"geo_named_area": {
                            u"country": u"Belgium",
                            u"something_else": False,
                        }
                    }
                ]
            }
        }
        with pytest.raises(jsonschema.ValidationError):
            schema.validate(nope)
