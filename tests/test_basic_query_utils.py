import pytest
from eevee.indexing.utils import DOC_TYPE
from mock import patch, MagicMock

from ckanext.versioned_datastore.lib.basic_query.utils import format_facets, get_fields
from ckanext.versioned_datastore.model import stats, slugs, details, downloads


@pytest.fixture
def with_versioned_datastore_tables(reset_db):
    '''
    Simple fixture which resets the database and creates the versioned-datastore tables.
    '''
    reset_db()
    tables = [
        stats.import_stats_table,
        slugs.datastore_slugs_table,
        details.datastore_resource_details_table,
        downloads.datastore_downloads_table,
    ]
    # create the tables if they don't exist
    for table in tables:
        if not table.exists():
            table.create()


class TestBasicQueryUtils(object):

    def test_format_facets(self):
        # first, check it can deal with an empty aggregation result
        assert format_facets({}) == {}

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

        assert len(facets) == 2
        assert facets[u'facet1'][u'details'][u'sum_other_doc_count'] == 901
        assert facets[u'facet1'][u'details'][u'doc_count_error_upper_bound'] == 12
        assert len(facets[u'facet1'][u'values']) == 5
        assert facets[u'facet1'][u'values'][u'value1'] == 43
        assert facets[u'facet1'][u'values'][u'value2'] == 243
        assert facets[u'facet1'][u'values'][u'value3'] == 543
        assert facets[u'facet1'][u'values'][u'value4'] == 143
        assert facets[u'facet1'][u'values'][u'value5'] == 743

        assert facets[u'facet2'][u'details'][u'sum_other_doc_count'] == 0
        assert facets[u'facet2'][u'details'][u'doc_count_error_upper_bound'] == 0
        assert len(facets[u'facet2'][u'values']) == 1
        assert facets[u'facet2'][u'values'][u'value1'] == 6

    @pytest.mark.filterwarnings(u'ignore::sqlalchemy.exc.SADeprecationWarning')
    @pytest.mark.ckan_config(u'ckan.plugins', u'versioned_datastore')
    @pytest.mark.usefixtures(u'with_versioned_datastore_tables', u'with_plugins')
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
            assert mapping == mock_mapping[u'beans-index']
            assert len(fields) == 3
            # the first field should always be the _id field and it should always be an integer type
            assert fields[0] == {u'id': u'_id', u'type': u'integer'}
            assert {u'id': u'field1', u'type': u'string'} in fields
            assert {u'id': u'field2', u'type': u'string'} in fields
