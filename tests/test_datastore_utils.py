from eevee.indexing.utils import DOC_TYPE
from mock import patch, MagicMock

from ckanext.versioned_datastore.lib.common import ALL_FORMATS, DATASTORE_ONLY_RESOURCE
from ckanext.versioned_datastore.lib.datastore_utils import is_datastore_resource, is_ingestible, \
    is_datastore_only_resource, iter_data_fields


class TestDatastoreUtils(object):

    def test_is_datastore_resource(self):
        scenarios = [
            (True, True, {u'beans-banana': MagicMock()}),
            (False, False, {}),
            (False, False, {u'beans-banana': MagicMock()}),
            (False, True, {}),
            (False, True, {u'banana': MagicMock()}),
            (False, True, {u'beans-cheese': MagicMock()}),
        ]

        def prefix_mock(name):
            return u'beans-{}'.format(name)

        for expected_outcome, exists, status in scenarios:
            client_mock = MagicMock(indices=MagicMock(exists=MagicMock(return_value=exists)))
            search_helper_mock = MagicMock(get_latest_index_versions=MagicMock(return_value=status))

            with patch(u'ckanext.versioned_datastore.lib.datastore_utils.prefix_resource',
                       new=prefix_mock):
                with patch(u'ckanext.versioned_datastore.lib.common.SEARCH_HELPER',
                           new=search_helper_mock):
                    with patch(u'ckanext.versioned_datastore.lib.common.ES_CLIENT', new=client_mock):
                        assert expected_outcome == is_datastore_resource(u'banana')

    def test_is_datastore_only_resource(self):
        for yes in [DATASTORE_ONLY_RESOURCE, u'http://{}'.format(DATASTORE_ONLY_RESOURCE),
                    u'https://{}'.format(DATASTORE_ONLY_RESOURCE)]:
            assert is_datastore_only_resource(yes)

        for no in [u'ftp://{}'.format(DATASTORE_ONLY_RESOURCE), u'this is datastore only',
                   None, u'{}/{}'.format(DATASTORE_ONLY_RESOURCE, DATASTORE_ONLY_RESOURCE),
                   u'https://{}/nope'.format(DATASTORE_ONLY_RESOURCE)]:
            assert not is_datastore_only_resource(no)

    def test_is_ingestible(self):
        # all formats should be ingestible (even in uppercase)
        for fmt in ALL_FORMATS:
            assert is_ingestible({u'format': fmt, u'url': MagicMock()})
            assert is_ingestible({u'format': fmt.upper(), u'url': MagicMock()})
        # zip should be ingestible (even in uppercase)
        assert is_ingestible({u'format': u'ZIP', u'url': MagicMock()})
        assert is_ingestible({u'format': u'zip', u'url': MagicMock()})
        # a datastore only resource should be ingestible
        assert is_ingestible({u'format': None, u'url': DATASTORE_ONLY_RESOURCE})

        # if there's no url then the resource is not ingestible
        assert not is_ingestible({u'url': None})
        assert not is_ingestible({u'format': u'csv', u'url': None})
        # if there's no format and the resource is not datastore only then it is not ingestible
        assert not is_ingestible({u'format': None, u'url': u'http://banana.com/test.csv'})

    def test_is_iter_data_fields(self):
        id_config = MagicMock()
        banana_config = MagicMock()
        llama_config = MagicMock()
        cheese_config = MagicMock()

        mapping = {
            u'mappings': {
                DOC_TYPE: {
                    u'properties': {
                        u'data': {
                            u'properties': {
                                u'_id': id_config,
                                u'nests': {
                                    u'properties': {
                                        u'banana': banana_config,
                                        u'llama': llama_config,
                                    }
                                },
                                u'cheese': cheese_config,
                            }
                        }
                    }
                }
            }
        }

        fields_and_configs = dict(iter_data_fields(mapping))
        assert fields_and_configs[(u'_id',)] == id_config
        assert fields_and_configs[(u'nests', u'banana')] == banana_config
        assert fields_and_configs[(u'nests', u'llama')] == llama_config
        assert fields_and_configs[(u'cheese',)] == cheese_config
