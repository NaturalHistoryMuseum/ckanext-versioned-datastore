import json
import os
import shutil
from uuid import uuid4

import pytest
from mock import patch

from ckanext.versioned_datastore.lib.downloads.derivatives.dwc import urls
from ckanext.versioned_datastore.lib.downloads.derivatives.dwc.schema import Schema

schema_cache = '/tmp/schema_cache'


class TestDwcSchemaSerialisation:
    def setup_method(self):
        os.mkdir(schema_cache)

    def teardown_method(self):
        shutil.rmtree(schema_cache)

    def test_can_serialise_schema(self):
        schema = Schema.regenerate()
        serialised_schema = schema.serialise()
        assert isinstance(serialised_schema, dict)
        assert 'domains' in serialised_schema
        assert 'props' in serialised_schema
        assert (
            serialised_schema['row_type'] == 'http://rs.tdwg.org/dwc/terms/Occurrence'
        )
        assert 'core_extension' not in serialised_schema

    def test_can_save_schema(self):
        schema = Schema.regenerate()
        fn = os.path.extsep.join([uuid4().hex, 'json'])
        fp = os.path.join(schema_cache, fn)
        schema.save(fp)
        assert os.path.exists(fp)

        with open(fp, 'r') as f:
            serialised_schema = json.load(f)

        assert isinstance(serialised_schema, dict)
        assert 'domains' in serialised_schema
        assert 'props' in serialised_schema
        assert (
            serialised_schema['row_type'] == 'http://rs.tdwg.org/dwc/terms/Occurrence'
        )

    def test_can_load_schema(self):
        schema = Schema.regenerate()
        fn = os.path.extsep.join([uuid4().hex, 'json'])
        fp = os.path.join(schema_cache, fn)
        schema.save(fp)
        assert os.path.exists(fp)

        with patch(
            'ckanext.versioned_datastore.lib.downloads.derivatives.dwc.schema.Schema.regenerate'
        ) as regenerate:
            loaded_schema = Schema.load(fp)
            regenerate.assert_not_called()

        assert isinstance(loaded_schema, Schema)
        assert loaded_schema.row_type == 'http://rs.tdwg.org/dwc/terms/Occurrence'

    def test_regenerates_if_not_exists(self):
        fn = os.path.extsep.join([uuid4().hex, 'json'])
        fp = os.path.join(schema_cache, fn)
        assert not os.path.exists(fp)

        with patch(
            'ckanext.versioned_datastore.lib.downloads.derivatives.dwc.schema.Schema.regenerate'
        ) as regenerate:
            loaded_schema = Schema.load(fp)
            regenerate.assert_called()

    @pytest.mark.parametrize('core_extension_url', list(urls.core_extensions.values()))
    def test_serialise_core_extension(self, core_extension_url):
        schema = Schema.regenerate(core_extension_url=core_extension_url)

        assert schema.core_extension.location.url == core_extension_url.url

        serialised_schema = schema.serialise()
        assert (
            serialised_schema['core_extension']['location']['url']
            == core_extension_url.url
        )

    def test_load_serialised_core_extension(self):
        core_extension_url = urls.core_extensions['gbif_occurrence']
        schema = Schema.regenerate(core_extension_url=core_extension_url)
        fn = os.path.extsep.join([uuid4().hex, 'json'])
        fp = os.path.join(schema_cache, fn)
        schema.save(fp)
        assert os.path.exists(fp)

        loaded_schema = Schema.load(fp)

        assert loaded_schema.core_extension.location.url == core_extension_url.url

    @pytest.mark.parametrize('extension_url', list(urls.extensions.values()))
    def test_serialise_extensions(self, extension_url):
        schema = Schema.regenerate(extension_urls=[extension_url])

        assert len(schema.extensions) == 1
        assert schema.extensions[0].location.url == extension_url.url

        serialised_schema = schema.serialise()
        assert len(serialised_schema['extensions']) == 1
        assert (
            serialised_schema['extensions'][0]['location']['url'] == extension_url.url
        )

    def test_load_serialised_extensions(self):
        extension_url = urls.extensions['gbif_multimedia']
        schema = Schema.regenerate(extension_urls=[extension_url])
        fn = os.path.extsep.join([uuid4().hex, 'json'])
        fp = os.path.join(schema_cache, fn)
        schema.save(fp)
        assert os.path.exists(fp)

        loaded_schema = Schema.load(fp)

        assert len(loaded_schema.extensions) == 1
        assert loaded_schema.extensions[0].location.url == extension_url.url
