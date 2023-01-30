import pytest
from mock import patch, MagicMock
from splitgill.indexing.utils import DOC_TYPE

from ckanext.versioned_datastore.lib.basic_query.utils import format_facets, get_fields


class TestBasicQueryUtils(object):
    def test_format_facets(self):
        # first, check it can deal with an empty aggregation result
        assert format_facets({}) == {}

        facets = format_facets(
            {
                'facet1': {
                    'sum_other_doc_count': 901,
                    'doc_count_error_upper_bound': 12,
                    'buckets': [
                        {'key': 'value1', 'doc_count': 43},
                        {'key': 'value2', 'doc_count': 243},
                        {'key': 'value3', 'doc_count': 543},
                        {'key': 'value4', 'doc_count': 143},
                        {'key': 'value5', 'doc_count': 743},
                    ],
                },
                'facet2': {
                    'sum_other_doc_count': 0,
                    'doc_count_error_upper_bound': 0,
                    'buckets': [
                        {'key': 'value1', 'doc_count': 6},
                    ],
                },
            }
        )

        assert len(facets) == 2
        assert facets['facet1']['details']['sum_other_doc_count'] == 901
        assert facets['facet1']['details']['doc_count_error_upper_bound'] == 12
        assert len(facets['facet1']['values']) == 5
        assert facets['facet1']['values']['value1'] == 43
        assert facets['facet1']['values']['value2'] == 243
        assert facets['facet1']['values']['value3'] == 543
        assert facets['facet1']['values']['value4'] == 143
        assert facets['facet1']['values']['value5'] == 743

        assert facets['facet2']['details']['sum_other_doc_count'] == 0
        assert facets['facet2']['details']['doc_count_error_upper_bound'] == 0
        assert len(facets['facet2']['values']) == 1
        assert facets['facet2']['values']['value1'] == 6

    @pytest.mark.filterwarnings('ignore::sqlalchemy.exc.SADeprecationWarning')
    @pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
    @pytest.mark.usefixtures('with_versioned_datastore_tables', 'with_plugins')
    def test_get_fields(self):
        mock_mapping = {
            u"beans-index": {
                u"mappings": {
                    DOC_TYPE: {
                        u"properties": {
                            u"data": {
                                u"properties": {
                                    u"_id": {'type': 'long'},
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
        prefix_mock = lambda name: f'beans-{name}'
        client_mock = MagicMock(indices=MagicMock(get_mapping=mapping_mock_function))
        search_helper_mock = MagicMock()
        es_response = [
            MagicMock(hits=MagicMock(total=4)),
            MagicMock(hits=MagicMock(total=10)),
        ]
        multisearch_mock = MagicMock()
        multisearch_mock.configure_mock(
            add=MagicMock(return_value=multisearch_mock),
            execute=MagicMock(return_value=es_response),
        )
        multisearch_class_mock = MagicMock(return_value=multisearch_mock)

        with patch(
            'ckanext.versioned_datastore.lib.basic_query.utils.prefix_resource',
            new=prefix_mock,
        ), patch(
            'ckanext.versioned_datastore.lib.common.ES_CLIENT', new=client_mock
        ), patch(
            'ckanext.versioned_datastore.lib.common.SEARCH_HELPER',
            new=search_helper_mock,
        ), patch(
            'ckanext.versioned_datastore.lib.basic_query.utils.MultiSearch',
            new=multisearch_class_mock,
        ):
            mapping, fields = get_fields('index')
            assert mapping == mock_mapping['beans-index']
            assert len(fields) == 3
            # the first field should always be the _id field and it should always be an integer type
            assert fields[0] == {'id': '_id', 'type': 'integer'}
            assert {'id': 'field1', 'type': 'string'} in fields
            assert {'id': 'field2', 'type': 'string'} in fields
