import io
import json
import os
from collections import OrderedDict

import jsonschema
import pytest
from ckanext.versioned_datastore.lib.query import schema as schema_lib
from mock import MagicMock, patch


class TestQuery(object):
    def test_load_core_schema(self):
        schema, validator = schema_lib.load_core_schema('v1.0.0')

        schema_path = os.path.join(schema_lib.schema_base_path, 'v1.0.0', 'v1.0.0.json')
        with io.open(schema_path, 'r', encoding='utf-8') as f:
            assert schema == json.load(f)
        # the validator classes are created on the fly so we'll just assert that the returned
        # validator object has a validate method on it as that's all we really care about
        assert hasattr(validator, 'validate')

    def test_schema_base_path_is_absolute(self):
        # load_core_schema needs this to be absolute
        assert os.path.isabs(schema_lib.schema_base_path)

    def test_get_latest_query_version(self):
        test_schemas = OrderedDict(
            [
                ('v1.0.0', MagicMock()),
                ('v1.0.1', MagicMock()),
                ('v2.10.1', MagicMock()),
            ]
        )
        with patch(
            'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas
        ):
            assert schema_lib.get_latest_query_version() == 'v1.0.0'

    def test_validate_query_valid(self):
        test_schemas = {'v1.0.0': MagicMock(validate=MagicMock(return_value=True))}
        with patch(
            'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas
        ):
            assert schema_lib.validate_query(MagicMock(), 'v1.0.0')

    def test_validate_query_invalid_query_version(self):
        with patch('ckanext.versioned_datastore.lib.query.schema.schemas', {}):
            with pytest.raises(schema_lib.InvalidQuerySchemaVersionError):
                schema_lib.validate_query(MagicMock(), 'v3.0.0')

    def test_validate_query_invalid_query(self):
        test_schemas = {
            'v1.0.0': MagicMock(
                validate=MagicMock(side_effect=jsonschema.ValidationError('no!'))
            )
        }
        with patch(
            'ckanext.versioned_datastore.lib.query.schema.schemas', test_schemas
        ):
            with pytest.raises(jsonschema.ValidationError):
                schema_lib.validate_query(MagicMock(), 'v1.0.0')

    def test_translate_query_invalid(self):
        with patch('ckanext.versioned_datastore.lib.query.schema.schemas', {}):
            with pytest.raises(schema_lib.InvalidQuerySchemaVersionError):
                schema_lib.translate_query(MagicMock(), MagicMock())

    def test_register_schema(self):
        # remove any existing registered schemas
        with patch('ckanext.versioned_datastore.lib.query.schema.schemas', {}):
            schema_lib.register_schema('v1.0.1', MagicMock())
            assert list(schema_lib.schemas.keys()) == ['v1.0.1']
            schema_lib.register_schema('v1.0.0', MagicMock())
            assert list(schema_lib.schemas.keys()) == ['v1.0.0', 'v1.0.1']
            schema_lib.register_schema('v2.0.1', MagicMock())
            assert list(schema_lib.schemas.keys()) == ['v1.0.0', 'v1.0.1', 'v2.0.1']
