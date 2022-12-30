import time

import pytest
from ckan import plugins
from ckan.tests import factories, helpers
from mock import patch
from splitgill.indexing.utils import get_elasticsearch_client
from splitgill.mongo import get_mongo

from ckanext.versioned_datastore.lib import common
from ckanext.versioned_datastore.model import stats, slugs, details, downloads
from tests.helpers import utils


@pytest.fixture(scope='module')
def with_versioned_datastore_tables(reset_db):
    """
    Simple fixture which resets the database and creates the versioned-datastore tables.
    """
    # setup
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

    # test method here
    yield

    # teardown
    for table in tables[::-1]:
        if table.exists():
            table.drop()


@pytest.fixture(scope='module')
def clear_es_mongo():
    """
    Deletes all documents from mongo and elasticsearch.
    """
    # setup
    if not plugins.plugin_loaded('versioned_datastore'):
        plugins.load('versioned_datastore')

    # test method here
    yield

    # teardown
    with get_mongo(common.CONFIG, common.CONFIG.mongo_database) as mongo_client:
        cols = mongo_client.list_collection_names()
        for c in cols:
            mongo_client[c].drop()
    es_client = get_elasticsearch_client(
        common.CONFIG,
        sniff_on_start=True,
        sniffer_timeout=60,
        sniff_on_connection_fail=True,
        sniff_timeout=10,
        http_compress=False,
        timeout=30,
    )
    es_client.delete_by_query(
        index=common.CONFIG.search_default_indexes, body={'query': {'match_all': {}}}
    )


@pytest.fixture(scope='module')
def with_vds_resource(clear_es_mongo):
    """
    Adds some test data to the datastore.
    """
    # setup
    if not plugins.plugin_loaded('versioned_datastore'):
        plugins.load('versioned_datastore')

    # because user_show is called in datastore_upsert
    user = factories.Sysadmin()
    package = factories.Dataset()

    def queue_mock(task, request):
        return utils.sync_enqueue_job_thread(
            task, args=[request], queue='importing', title=str(request)
        )

    def get_action_mock(action_name):
        """
        Adds the user to the context.
        """

        def call_action_mock(context=None, data_dict=None):
            context = context or {}
            data_dict = data_dict or {}
            context['user'] = user['id']
            return helpers.call_action(action_name, context=context, **data_dict)

        return call_action_mock

    with patch('ckan.plugins.toolkit.get_action', get_action_mock), patch(
        'ckanext.versioned_datastore.lib.importing.queuing.queue', queue_mock
    ):
        resource = factories.Resource(
            package_id=package['id'],
            url_type='datastore',
            url=common.DATASTORE_ONLY_RESOURCE,
        )
    records = [
        {
            'scientificName': 'Boops boops',
            'img': 'img-url-here',
            'urlSlug': 'boops-boops',
        },
        {'scientificName': 'Felis catus', 'urlSlug': 'felis-catus'},
    ]
    helpers.call_action('datastore_create', resource_id=resource['id'], records=records)

    with patch(
        'ckanext.versioned_datastore.lib.importing.queuing.queue',
        queue_mock,
    ):
        helpers.call_action(
            'datastore_upsert',
            context={'user': user['id']},
            resource_id=resource['id'],
            records=records,
            replace=True,
        )

    # TODO: do this better somehow
    # even though we've replaced the queues with sync versions, the new data still seems
    # to be added to the datastore asynchronously
    wait_loop = 0
    while (
        wait_loop < 10
        and helpers.call_action('datastore_count', resource_ids=[resource['id']]) == 0
    ):
        wait_loop += 1
        time.sleep(2)

    # test method here
    yield resource
