import nose
from ckanext.versioned_datastore.lib.utils import (ALL_FORMATS, DATASTORE_ONLY_RESOURCE,
                                                   format_facets, get_fields, is_datastore_resource,
                                                   is_ingestible, validate)
from ckantest.models import TestBase
from eevee.indexing.utils import DOC_TYPE
from mock import patch, MagicMock, call


class TestUtils(TestBase):
    plugins = [u'versioned_datastore']
    validate_function = u'ckanext.versioned_datastore.lib.utils.toolkit.navl_validate'

    def test_validate_uses_context_schema(self):
        # mock the validate function to return a mock and False for errors
        mock_validate = MagicMock(return_value=(MagicMock(), False))
        with patch(self.validate_function, side_effect=mock_validate):
            context_schema = MagicMock()
            default_schema = MagicMock()
            data_dict = MagicMock()
            # create a context with a schema specified
            context = {
                u'schema': context_schema
                }
            # call validate
            validate(context, data_dict, default_schema)

            # check that the validate function was called with the context schema not the default
            # one
            nose.tools.assert_equal(mock_validate.call_args,
                                    call(data_dict, context_schema, context))

    def test_validate_uses_default_schema(self):
        mock_validate = MagicMock(return_value=(MagicMock(), False))
        with patch(self.validate_function, side_effect=mock_validate):
            default_schema = MagicMock()
            data_dict = MagicMock()
            # create a context with a no schema specified
            context = {}
            # call validate
            validate(context, data_dict, default_schema)

            # check that the validate function was called with the context schema not the default
            # one
            nose.tools.assert_equal(mock_validate.call_args,
                                    call(data_dict, default_schema, context))

    def test_validate_returns_validated_data_dict(self):
        # the validation can alter the data dict so we need to ensure that the validate function
        # returns
        # the data_dict passed back from `validate` not the one it was given as an argument
        returned_data_dict = MagicMock()
        # mock the validate function to return the data dict and False for errors
        mock_validate = MagicMock(return_value=(returned_data_dict, False))
        with patch(self.validate_function, side_effect=mock_validate):
            passed_data_dict = MagicMock()
            # check that validate returns the data dict we mock returned from the validate function
            # above not the other MagicMock we passed to it
            data_dict = validate({}, passed_data_dict, MagicMock())
            nose.tools.assert_equal(data_dict, returned_data_dict)
            nose.tools.assert_not_equal(data_dict, passed_data_dict)

    def test_format_facets(self):
        # first, check it can deal with an empty aggregation result
        nose.tools.assert_equal(format_facets({}), {})

        facets = format_facets({
            u'facet1': {
                u'sum_other_doc_count': 901,
                u'doc_count_error_upper_bound': 12,
                u'buckets': [
                    {
                        u'key': u'value1',
                        u'doc_count': 43
                        },
                    {
                        u'key': u'value2',
                        u'doc_count': 243
                        },
                    {
                        u'key': u'value3',
                        u'doc_count': 543
                        },
                    {
                        u'key': u'value4',
                        u'doc_count': 143
                        },
                    {
                        u'key': u'value5',
                        u'doc_count': 743
                        },
                    ]
                },
            u'facet2': {
                u'sum_other_doc_count': 0,
                u'doc_count_error_upper_bound': 0,
                u'buckets': [
                    {
                        u'key': u'value1',
                        u'doc_count': 6
                        },
                    ]
                }
            })

        nose.tools.assert_equal(len(facets), 2)
        nose.tools.assert_equal(facets[u'facet1'][u'details'][u'sum_other_doc_count'], 901)
        nose.tools.assert_equal(facets[u'facet1'][u'details'][u'doc_count_error_upper_bound'], 12)
        nose.tools.assert_equal(len(facets[u'facet1'][u'values']), 5)
        nose.tools.assert_equal(facets[u'facet1'][u'values'][u'value1'], 43)
        nose.tools.assert_equal(facets[u'facet1'][u'values'][u'value2'], 243)
        nose.tools.assert_equal(facets[u'facet1'][u'values'][u'value3'], 543)
        nose.tools.assert_equal(facets[u'facet1'][u'values'][u'value4'], 143)
        nose.tools.assert_equal(facets[u'facet1'][u'values'][u'value5'], 743)

        nose.tools.assert_equal(facets[u'facet2'][u'details'][u'sum_other_doc_count'], 0)
        nose.tools.assert_equal(facets[u'facet2'][u'details'][u'doc_count_error_upper_bound'], 0)
        nose.tools.assert_equal(len(facets[u'facet2'][u'values']), 1)
        nose.tools.assert_equal(facets[u'facet2'][u'values'][u'value1'], 6)

    def test_get_fields(self):
        mock_mapping = {
            u"beans-index": {
                u"mappings": {
                    DOC_TYPE: {
                        u"properties": {
                            u"data": {
                                u"properties": {
                                    u"_id": {
                                        u'type': u'long'
                                        },
                                    u"field1": {
                                        u"type": u"keyword",
                                        },
                                    u"field2": {
                                        u"type": u"date",
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            }

        mapping_mock_function = MagicMock(return_value=mock_mapping)
        prefix_mock = lambda name: u'beans-{}'.format(name)
        searcher_mock = MagicMock(elasticsearch=MagicMock(indices=MagicMock(get_mapping=
                                                                            mapping_mock_function)))
        es_response = [MagicMock(hits=MagicMock(total=4)), MagicMock(hits=MagicMock(total=10))]
        multisearch_mock = MagicMock()
        multisearch_mock.configure_mock(add=MagicMock(return_value=multisearch_mock),
                                        execute=MagicMock(return_value=es_response))
        multisearch_class_mock = MagicMock(return_value=multisearch_mock)

        with patch(u'ckanext.versioned_datastore.lib.utils.prefix_resource',
                        new=prefix_mock), \
             patch(u'ckanext.versioned_datastore.lib.utils.SEARCHER', new=searcher_mock), \
             patch(u'ckanext.versioned_datastore.lib.utils.MultiSearch',
                        new=multisearch_class_mock):
            mapping, fields = get_fields(u'index')
            nose.tools.assert_equal(mapping, mock_mapping[u'beans-index'])
            nose.tools.assert_equal(len(fields), 3)
            # the first field should always be the _id field and it should always be an integer type
            nose.tools.assert_equal(fields[0], {
                u'id': u'_id',
                u'type': u'integer'
                })
            nose.tools.assert_true({
                                       u'id': u'field1',
                                       u'type': u'string'
                                       } in fields)
            nose.tools.assert_true({
                                       u'id': u'field2',
                                       u'type': u'string'
                                       } in fields)

    def test_is_datastore_resource(self):
        exists_mock = MagicMock(return_value=True)
        prefix_mock = lambda name: u'beans-{}'.format(name)
        searcher_mock = MagicMock(elasticsearch=MagicMock(indices=MagicMock(exists=exists_mock)))

        with patch(u'ckanext.versioned_datastore.lib.utils.prefix_resource',
                        new=prefix_mock), \
             patch(u'ckanext.versioned_datastore.lib.utils.SEARCHER', new=searcher_mock):
            nose.tools.assert_true(is_datastore_resource(u'banana'))
            nose.tools.assert_equal(exists_mock.call_args, call(u'beans-banana'))

    def test_is_ingestible(self):
        for fmt in ALL_FORMATS:
            nose.tools.assert_true(is_ingestible({
                u'format': fmt,
                u'url': MagicMock()
                }))
            nose.tools.assert_true(is_ingestible({
                u'format': fmt.upper(),
                u'url': MagicMock()
                }))

        nose.tools.assert_true(is_ingestible({
            u'format': None,
            u'url': DATASTORE_ONLY_RESOURCE
            }))
        nose.tools.assert_false(is_ingestible({
            u'format': None,
            u'url': None
            }))
        nose.tools.assert_false(is_ingestible({
            u'format': None,
            u'url': u'http://banana.com/test.csv'
            }))
        nose.tools.assert_false(is_ingestible({
            u'format': u'csv',
            u'url': None
            }))
