import nose
from ckantest.models import TestBase
from eevee.indexing.utils import DOC_TYPE
from mock import patch, MagicMock

from ..lib.basic_query.utils import format_facets, get_fields


class TestBasicQueryUtils(TestBase):
    plugins = [u'versioned_datastore']

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
        client_mock = MagicMock(indices=MagicMock(get_mapping=mapping_mock_function))
        search_helper_mock = MagicMock()
        es_response = [MagicMock(hits=MagicMock(total=4)), MagicMock(hits=MagicMock(total=10))]
        multisearch_mock = MagicMock()
        multisearch_mock.configure_mock(add=MagicMock(return_value=multisearch_mock),
                                        execute=MagicMock(return_value=es_response))
        multisearch_class_mock = MagicMock(return_value=multisearch_mock)

        with patch(u'ckanext.versioned_datastore.lib.basic_query.utils.prefix_resource',
                   new=prefix_mock), \
            patch(u'ckanext.versioned_datastore.lib.common.ES_CLIENT', new=client_mock), \
            patch(u'ckanext.versioned_datastore.lib.common.SEARCH_HELPER',
                  new=search_helper_mock), \
            patch(u'ckanext.versioned_datastore.lib.basic_query.utils.MultiSearch',
                  new=multisearch_class_mock):
            mapping, fields = get_fields(u'index')
            nose.tools.assert_equal(mapping, mock_mapping[u'beans-index'])
            nose.tools.assert_equal(len(fields), 3)
            # the first field should always be the _id field and it should always be an integer type
            nose.tools.assert_equal(fields[0], {
                u'id': u'_id',
                u'type': u'integer'
            })
            nose.tools.assert_true({u'id': u'field1', u'type': u'string'} in fields)
            nose.tools.assert_true({u'id': u'field2', u'type': u'string'} in fields)
