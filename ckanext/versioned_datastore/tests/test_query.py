import io
import json
from collections import OrderedDict

import jsonschema
import os
from ckantest.models import TestBase
from mock import MagicMock, patch
from nose.tools import assert_equal, assert_raises, assert_true

from ..lib.query import schema as schema_lib


class TestQuery(TestBase):
    plugins = [u'versioned_datastore']

    def test_load_core_schema(self):
        schema, validator = schema_lib.load_core_schema(u'v1.0.0')

        schema_path = os.path.join(schema_lib.schema_base_path, u'v1.0.0', u'v1.0.0.json')
        with io.open(schema_path, u'r', encoding=u'utf-8') as f:
            assert_equal(schema, json.load(f))
        # the validator classes are created on the fly so we'll just assert that the returned
        # validator object has a validate method on it as that's all we really care about
        assert_true(hasattr(validator, u'validate'))

    def test_schema_base_path_is_absolute(self):
        # load_core_schema needs this to be absolute
        assert_true(os.path.isabs(schema_lib.schema_base_path))

    def test_get_latest_query_version(self):
        test_schemas = OrderedDict([
            (u'v1.0.0', MagicMock()),
            (u'v1.0.1', MagicMock()),
            (u'v2.10.1', MagicMock()),
        ])
        with patch(u'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas):
            assert_equal(schema_lib.get_latest_query_version(), u'v1.0.0')

    def test_validate_query_valid(self):
        test_schemas = {
            u'v1.0.0': MagicMock(validate=MagicMock(return_value=True))
        }
        with patch(u'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas):
            assert_true(schema_lib.validate_query(MagicMock(), u'v1.0.0'))

    def test_validate_query_invalid_query_version(self):
        with patch(u'ckanext.versioned_datastore.lib.query.schema.schemas', {}):
            with assert_raises(schema_lib.InvalidQuerySchemaVersionError):
                schema_lib.validate_query(MagicMock(), u'v3.0.0')

    def test_validate_query_invalid_query(self):
        test_schemas = {
            u'v1.0.0': MagicMock(validate=MagicMock(side_effect=jsonschema.ValidationError(u'no!')))
        }
        with patch(u'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas):
            with assert_raises(jsonschema.ValidationError):
                schema_lib.validate_query(MagicMock(), u'v1.0.0')

    def test_translate_query_invalid(self):
        with patch(u'ckanext.versioned_datastore.lib.query.schema.schemas', {}):
            with assert_raises(schema_lib.InvalidQuerySchemaVersionError):
                schema_lib.translate_query(MagicMock(), MagicMock())

    def test_register_schema(self):
        # remove any existing registered schemas
        schema_lib.schemas = {}

        schema_lib.register_schema(u'v1.0.1', MagicMock())
        assert_equal(list(schema_lib.schemas.keys()), [u'v1.0.1'])
        schema_lib.register_schema(u'v1.0.0', MagicMock())
        assert_equal(list(schema_lib.schemas.keys()), [u'v1.0.0', u'v1.0.1'])
        schema_lib.register_schema(u'v2.0.1', MagicMock())
        assert_equal(list(schema_lib.schemas.keys()), [u'v1.0.0', u'v1.0.1', u'v2.0.1'])
