import pytest
from mock import MagicMock
from splitgill.model import Record

from ckanext.versioned_datastore.lib.common import ALL_FORMATS, DATASTORE_ONLY_RESOURCE
from ckanext.versioned_datastore.lib.importing.options import (
    create_default_options_builder,
)
from ckanext.versioned_datastore.lib.utils import (
    get_database,
    is_datastore_only_resource,
    is_datastore_resource,
    is_ingestible,
)


@pytest.mark.usefixtures('with_vds')
def test_is_datastore_resource():
    assert not is_datastore_resource('eggs')

    # manually add some data to the database
    database = get_database('eggs')
    database.update_options(create_default_options_builder().build(), commit=False)
    database.ingest([Record.new({'beans': 'yes please'})], commit=True)
    database.sync()
    # there should be data now so this should be recognised as a datastore resource
    assert is_datastore_resource('eggs')


def test_is_datastore_only_resource():
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


def test_is_ingestible():
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
