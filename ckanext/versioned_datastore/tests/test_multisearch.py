import json
from collections import OrderedDict

import jsonschema
import os

from ckanext.versioned_datastore.lib.multisearch import load_schemas, get_latest_query_version, \
    validate_query, InvalidQuerySchemaVersionError, translate_query
from ckantest.models import TestBase
from mock import MagicMock, patch
from nose.tools import assert_equal, assert_raises, assert_true


class TestMultisearch(TestBase):
    plugins = [u'versioned_datastore']

    def test_load_schemas_default(self):
        schemas = load_schemas()
        for version, validator in schemas.items():
            assert_true(isinstance(version, unicode))
            # the validator classes are created on the fly so we'll just assert that the returned
            # validator object has a validate method on it as that's all we really care about
            assert_true(hasattr(validator, u'validate'))

    def test_load_schemas_default_path_is_the_public_folder(self):
        parent_path = os.path.dirname(__file__)
        schema_base_path = os.path.join(parent_path, u'..', u'theme', u'public', u'schemas')
        default_schemas = load_schemas()
        also_default_schemas = load_schemas(schema_base_path=schema_base_path)

        for (version1, validator1), (version2, validator2) in zip(default_schemas.items(),
                                                                  also_default_schemas.items()):
            assert_equal(version1, version2)
            assert_equal(validator1.schema, validator2.schema)

    def test_load_schemas_from_somewhere_else(self):
        parent_path = os.path.dirname(__file__)
        schema_base_path = os.path.join(parent_path, u'testSchemas')
        schemas = load_schemas(schema_base_path=schema_base_path)

        assert_equal(list(schemas.keys()), [u'v0.1.3', u'v1.9.3', u'v1.70.0', u'v4.0.0'])
        for version, validator in schemas.items():
            with open(os.path.join(schema_base_path, u'{}.json'.format(version)), u'r') as f:
                schema = json.load(f)
                assert_equal(validator.schema, schema)

    def test_get_latest_query_version(self):
        test_schemas = OrderedDict([
            (u'v1.0.0', MagicMock()),
            (u'v1.0.1', MagicMock()),
            (u'v2.10.1', MagicMock()),
        ])
        with patch(u'ckanext.versioned_datastore.lib.multisearch.schemas', test_schemas):
            assert_equal(get_latest_query_version(), u'v1.0.0')

    def test_validate_query_valid(self):
        test_schemas = {
            u'v1.0.0': MagicMock(validate=MagicMock(return_value=True))
        }
        with patch(u'ckanext.versioned_datastore.lib.multisearch.schemas', test_schemas):
            assert_true(validate_query(MagicMock(), u'v1.0.0'))

    def test_validate_query_invalid_query_version(self):
        with patch(u'ckanext.versioned_datastore.lib.multisearch.schemas', {}):
            with assert_raises(InvalidQuerySchemaVersionError):
                validate_query(MagicMock(), u'v3.0.0')

    def test_validate_query_invalid_query(self):
        test_schemas = {
            u'v1.0.0': MagicMock(validate=MagicMock(side_effect=jsonschema.ValidationError(u'no!')))
        }
        with patch(u'ckanext.versioned_datastore.lib.multisearch.schemas', test_schemas):
            with assert_raises(jsonschema.ValidationError):
                validate_query(MagicMock(), u'v1.0.0')

    def test_translate_query_invalid(self):
        with patch(u'ckanext.versioned_datastore.lib.multisearch.schemas', {}):
            with assert_raises(InvalidQuerySchemaVersionError):
                translate_query(MagicMock(), MagicMock())


class TestV1_0_0Translator(TestBase):
    plugins = [u'versioned_datastore']

    @staticmethod
    def compare_query_and_search(query, search_dict):
        assert_equal(translate_query(query, u'v1.0.0').to_dict(), search_dict)

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
                    u'must': [
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
                    u'must': [
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
                        },
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
                    u'must': [
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
