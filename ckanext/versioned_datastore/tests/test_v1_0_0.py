# !/usr/bin/env python
# encoding: utf-8
import io
import os

from ckanext.versioned_datastore.lib.query import schema_base_path
from ckanext.versioned_datastore.lib.query.v1_0_0 import v1_0_0Schema
from ckantest.models import TestBase
from nose.tools import assert_equal


class TestV1_0_0Translator(TestBase):
    plugins = [u'versioned_datastore']

    @staticmethod
    def compare_query_and_search(query, search_dict):
        schema = v1_0_0Schema()
        assert_equal(schema.translate(query).to_dict(), search_dict)

    def test_translate_1(self):
        query = {}
        search_dict = {}
        self.compare_query_and_search(query, search_dict)

    def test_translate_2(self):
        query = {
            u'search': u'mollusca'
        }
        search_dict = {
            u'query': {
                u'match': {
                    u'meta.all': {
                        u'query': u'mollusca',
                        u'operator': u'and'
                    }
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_3(self):
        query = {
            u"filters": {
                u"and": [
                    {
                        u"string_equals": {
                            u"fields": [
                                u"genus"
                            ],
                            u"value": u"helix"
                        }
                    },
                    {
                        u"string_contains": {
                            u"fields": [
                                u"higherGeography"
                            ],
                            u"value": u"europe"
                        }
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'bool': {
                    u'filter': [
                        {
                            u'term': {
                                u'data.genus': u'helix'
                            }
                        },
                        {
                            u'match': {
                                u'data.higherGeography.full': {
                                    u'query': u'europe',
                                    u'operator': u'and'
                                }
                            }
                        }
                    ]
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_4(self):
        query = {
            u'search': u'italy',
            u"filters": {
                u"and": [
                    {
                        u"string_equals": {
                            u"fields": [
                                u"genus"
                            ],
                            u"value": u"helix"
                        }
                    },
                    {
                        u"string_contains": {
                            u"fields": [
                                u"higherGeography"
                            ],
                            u"value": u"europe"
                        }
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'bool': {
                    u'filter': [
                        {
                            u'term': {
                                u'data.genus': u'helix'
                            }
                        },
                        {
                            u'match': {
                                u'data.higherGeography.full': {
                                    u'query': u'europe',
                                    u'operator': u'and'
                                }
                            }
                        }
                    ],
                    u'must': [
                        {
                            u'match': {
                                u'meta.all': {
                                    u'query': u'italy',
                                    u'operator': u'and'
                                }
                            }
                        }
                    ]
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_5(self):
        query = {
            u"filters": {
                u"and": [
                    {
                        u"string_equals": {
                            u"fields": [
                                u"genus"
                            ],
                            u"value": u"helix"
                        }
                    },
                    {
                        u"or": [
                            {
                                u"string_contains": {
                                    u"fields": [
                                        u"higherGeography"
                                    ],
                                    u"value": u"italy"
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [
                                        u"higherGeography"
                                    ],
                                    u"value": u"spain"
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [
                                        u"higherGeography"
                                    ],
                                    u"value": u"portugal"
                                }
                            }
                        ]
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'bool': {
                    u'filter': [
                        {
                            u'term': {
                                u'data.genus': u'helix'
                            }
                        },
                        {
                            u'bool': {
                                u'minimum_should_match': 1,
                                u'should': [
                                    {
                                        u'match': {
                                            u'data.higherGeography.full': {
                                                u'query': u'italy',
                                                u'operator': u'and'
                                            }
                                        }
                                    },
                                    {
                                        u'match': {
                                            u'data.higherGeography.full': {
                                                u'query': u'spain',
                                                u'operator': u'and'
                                            }
                                        }
                                    },
                                    {
                                        u'match': {
                                            u'data.higherGeography.full': {
                                                u'query': u'portugal',
                                                u'operator': u'and'
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_6(self):
        query = {
            u"filters": {
                u"and": [
                    {
                        u"string_equals": {
                            u"fields": [
                                u"genus"
                            ],
                            u"value": u"helix"
                        }
                    },
                    {
                        u"number_range": {
                            u"fields": [
                                u"year"
                            ],
                            u"less_than": 2010,
                            u"less_than_inclusive": True,
                            u"greater_than": 2000,
                            u"greater_than_inclusive": True
                        }
                    },
                    {
                        u"or": [
                            {
                                u"string_contains": {
                                    u"fields": [
                                        u"higherGeography"
                                    ],
                                    u"value": u"italy"
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [
                                        u"higherGeography"
                                    ],
                                    u"value": u"spain"
                                }
                            },
                            {
                                u"string_contains": {
                                    u"fields": [
                                        u"higherGeography"
                                    ],
                                    u"value": u"portugal"
                                }
                            }
                        ]
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'bool': {
                    u'filter': [
                        {
                            u'term': {
                                u'data.genus': u'helix'
                            }
                        },
                        {
                            u'range': {
                                u'data.year.number': {
                                    u'gte': 2000,
                                    u'lte': 2010
                                }
                            }
                        },
                        {
                            u'bool': {
                                u'minimum_should_match': 1,
                                u'should': [
                                    {
                                        u'match': {
                                            u'data.higherGeography.full': {
                                                u'query': u'italy',
                                                u'operator': u'and'
                                            }
                                        }
                                    },
                                    {
                                        u'match': {
                                            u'data.higherGeography.full': {
                                                u'query': u'spain',
                                                u'operator': u'and'
                                            }
                                        }
                                    },
                                    {
                                        u'match': {
                                            u'data.higherGeography.full': {
                                                u'query': u'portugal',
                                                u'operator': u'and'
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_7(self):
        query = {
            u"filters": {
                u"and": [
                    {
                        u"string_equals": {
                            u"fields": [
                                u"genus"
                            ],
                            u"value": u"helix"
                        }
                    },
                    {
                        u"geo_point": {
                            u"latitude": 51.4712,
                            u"longitude": -0.9421,
                            u"radius": 10,
                            u"radius_unit": u"mi"
                        }
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'bool': {
                    u'filter': [
                        {
                            u'term': {
                                u'data.genus': u'helix'
                            }
                        },
                        {
                            u'geo_distance': {
                                u'meta.geo': {
                                    u'lat': 51.4712,
                                    u'lon': -0.9421
                                },
                                u'distance': u'10mi'
                            }
                        }
                    ]
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_8(self):
        query = {
            u"filters": {
                u"and": [
                    {
                        u"exists": {
                            u"fields": [
                                u"associatedMedia"
                            ]
                        }
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'exists': {
                    u'field': u'data.associatedMedia'
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_9(self):
        query = {
            u"filters": {
                u"and": [
                    {
                        u"exists": {
                            u"geo_field": True
                        }
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'exists': {
                    u'field': u'meta.geo'
                }
            }
        }
        self.compare_query_and_search(query, search_dict)

    def test_translate_10(self):
        country = u'Curaçao'
        multipolygon = v1_0_0Schema().geojson[u'country'][country]
        query = {
            u"filters": {
                u"and": [
                    {
                        u"geo_named_area": {
                            u"country": country
                        }
                    }
                ]
            }
        }
        search_dict = {
            u'query': {
                u'geo_polygon': {
                    u'meta.geo': {
                        u'points': [
                            {u'lat': point[1], u'lon': point[0]} for point in multipolygon[0][0]
                        ]
                    }
                }
            }
        }
        self.compare_query_and_search(query, search_dict)
