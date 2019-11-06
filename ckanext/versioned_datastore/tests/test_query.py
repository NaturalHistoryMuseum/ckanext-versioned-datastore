import io
import json
from collections import OrderedDict

import jsonschema
import os
from ckanext.versioned_datastore.lib import query
from ckantest.models import TestBase
from mock import MagicMock, patch
from nose.tools import assert_equal, assert_raises, assert_true


class TestQuery(TestBase):
    plugins = [u'versioned_datastore']

    def test_load_core_schema(self):
        schema, validator = query.load_core_schema(u'v1.0.0')

        schema_path = os.path.join(query.schema_base_path, u'v1.0.0.json')
        with io.open(schema_path, u'r', encoding=u'utf-8') as f:
            assert_equal(schema, json.load(f))
        # the validator classes are created on the fly so we'll just assert that the returned
        # validator object has a validate method on it as that's all we really care about
        assert_true(hasattr(validator, u'validate'))

    def test_get_latest_query_version(self):
        test_schemas = OrderedDict([
            (u'v1.0.0', MagicMock()),
            (u'v1.0.1', MagicMock()),
            (u'v2.10.1', MagicMock()),
        ])
        with patch(u'ckanext.versioned_datastore.lib.query.schemas', test_schemas):
            assert_equal(query.get_latest_query_version(), u'v1.0.0')

    def test_validate_query_valid(self):
        test_schemas = {
            u'v1.0.0': MagicMock(validate=MagicMock(return_value=True))
        }
        with patch(u'ckanext.versioned_datastore.lib.query.schemas', test_schemas):
            assert_true(query.validate_query(MagicMock(), u'v1.0.0'))

    def test_validate_query_invalid_query_version(self):
        with patch(u'ckanext.versioned_datastore.lib.query.schemas', {}):
            with assert_raises(query.InvalidQuerySchemaVersionError):
                query.validate_query(MagicMock(), u'v3.0.0')

    def test_validate_query_invalid_query(self):
        test_schemas = {
            u'v1.0.0': MagicMock(validate=MagicMock(side_effect=jsonschema.ValidationError(u'no!')))
        }
        with patch(u'ckanext.versioned_datastore.lib.query.schemas', test_schemas):
            with assert_raises(jsonschema.ValidationError):
                query.validate_query(MagicMock(), u'v1.0.0')

    def test_translate_query_invalid(self):
        with patch(u'ckanext.versioned_datastore.lib.query.schemas', {}):
            with assert_raises(query.InvalidQuerySchemaVersionError):
                query.translate_query(MagicMock(), MagicMock())

    def test_register_schema(self):
        # remove any existing registered schemas
        query.schemas = {}

        query.register_schema(u'v1.0.1', MagicMock())
        assert_equal(list(query.schemas.keys()), [u'v1.0.1'])
        query.register_schema(u'v1.0.0', MagicMock())
        assert_equal(list(query.schemas.keys()), [u'v1.0.0', u'v1.0.1'])
        query.register_schema(u'v2.0.1', MagicMock())
        assert_equal(list(query.schemas.keys()), [u'v1.0.0', u'v1.0.1', u'v2.0.1'])

