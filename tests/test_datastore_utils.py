from splitgill.indexing.utils import DOC_TYPE
from mock import patch, MagicMock

from ckanext.versioned_datastore.lib.common import ALL_FORMATS, DATASTORE_ONLY_RESOURCE
from ckanext.versioned_datastore.lib.datastore_utils import (
    is_datastore_resource,
    is_ingestible,
    is_datastore_only_resource,
    iter_data_fields,
)


class TestDatastoreUtils(object):
    def test_is_datastore_resource(self):
        scenarios = [
            (True, True, {'beans-banana': MagicMock()}),
            (False, False, {}),
            (False, False, {'beans-banana': MagicMock()}),
            (False, True, {}),
            (False, True, {'banana': MagicMock()}),
            (False, True, {'beans-cheese': MagicMock()}),
        ]

        def prefix_mock(name):
            return f'beans-{name}'

        for expected_outcome, exists, status in scenarios:
            client_mock = MagicMock(
                indices=MagicMock(exists=MagicMock(return_value=exists))
            )
            search_helper_mock = MagicMock(
                get_latest_index_versions=MagicMock(return_value=status)
            )

            with patch(
                'ckanext.versioned_datastore.lib.datastore_utils.prefix_resource',
                new=prefix_mock,
            ):
                with patch(
                    'ckanext.versioned_datastore.lib.common.SEARCH_HELPER',
                    new=search_helper_mock,
                ):
                    with patch(
                        'ckanext.versioned_datastore.lib.common.ES_CLIENT',
                        new=client_mock,
                    ):
                        assert expected_outcome == is_datastore_resource('banana')

    def test_is_datastore_only_resource(self):
        for yes in [
            DATASTORE_ONLY_RESOURCE,
            f'http://{DATASTORE_ONLY_RESOURCE}',
            f'https://{DATASTORE_ONLY_RESOURCE}',
        ]:
            assert is_datastore_only_resource(yes)

        for no in [
            f'ftp://{DATASTORE_ONLY_RESOURCE}',
            'this is datastore only',
            None,
            f'{DATASTORE_ONLY_RESOURCE}/{DATASTORE_ONLY_RESOURCE}',
            f'https://{DATASTORE_ONLY_RESOURCE}/nope',
        ]:
            assert not is_datastore_only_resource(no)

    def test_is_ingestible(self):
        # all formats should be ingestible (even in uppercase)
        for fmt in ALL_FORMATS:
            assert is_ingestible({'format': fmt, 'url': MagicMock()})
            assert is_ingestible({'format': fmt.upper(), 'url': MagicMock()})
        # zip should be ingestible (even in uppercase)
        assert is_ingestible({'format': 'ZIP', 'url': MagicMock()})
        assert is_ingestible({'format': 'zip', 'url': MagicMock()})
        # a datastore only resource should be ingestible
        assert is_ingestible({'format': None, 'url': DATASTORE_ONLY_RESOURCE})

        # if there's no url then the resource is not ingestible
        assert not is_ingestible({'url': None})
        assert not is_ingestible({'format': 'csv', 'url': None})
        # if there's no format and the resource is not datastore only then it is not ingestible
        assert not is_ingestible({'format': None, 'url': 'http://banana.com/test.csv'})

    def test_is_iter_data_fields(self):
        id_config = MagicMock()
        banana_config = MagicMock()
        llama_config = MagicMock()
        cheese_config = MagicMock()

        mapping = {
            'mappings': {
                DOC_TYPE: {
                    'properties': {
                        'data': {
                            'properties': {
                                '_id': id_config,
                                'nests': {
                                    'properties': {
                                        'banana': banana_config,
                                        'llama': llama_config,
                                    }
                                },
                                'cheese': cheese_config,
                            }
                        }
                    }
                }
            }
        }

        fields_and_configs = dict(iter_data_fields(mapping))
        assert fields_and_configs[('_id',)] == id_config
        assert fields_and_configs[('nests', 'banana')] == banana_config
        assert fields_and_configs[('nests', 'llama')] == llama_config
        assert fields_and_configs[('cheese',)] == cheese_config
