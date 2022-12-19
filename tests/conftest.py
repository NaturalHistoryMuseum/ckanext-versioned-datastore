import pytest
from ckan.tests import factories
from collections import namedtuple
from mock import patch

from ckanext.versioned_datastore.model import stats, slugs, details, downloads


@pytest.fixture(scope='module')
def with_versioned_datastore_tables(reset_db):
    """
    Simple fixture which resets the database and creates the versioned-datastore tables.
    """
    reset_db()
    tables = [
        stats.import_stats_table,
        slugs.datastore_slugs_table,
        details.datastore_resource_details_table,
        downloads.datastore_downloads_core_files_table,
        downloads.datastore_downloads_derivative_files_table,
        downloads.datastore_downloads_requests_table,
    ]
    # create the tables if they don't exist
    for table in tables:
        if not table.exists():
            table.create()


@pytest.fixture(scope='module')
def patch_elasticsearch_scan():
    """
    Fixture that patches elasticsearch_dsl.Search.scan to return a test resource.
    """
    MockHit = namedtuple('MockHit', ['name'])
    resource_dict = factories.Resource()
    with patch(
        'ckanext.versioned_datastore.lib.query.utils.Search.scan',
        return_value=[MockHit(name=resource_dict['id'])],
    ) as m:
        yield m
