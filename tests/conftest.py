import pytest
from ckan import plugins
from ckan.tests import factories, helpers
from mock import patch

from ckanext.versioned_datastore.model import stats, slugs, details, downloads
from tests.helpers import utils


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
def with_vds_resource():
    plugins.load('versioned_datastore')
    # because user_show is called in datastore_upsert
    user = factories.Sysadmin()
    package = factories.Dataset()

    def queue_mock(task, request):
        return utils.sync_enqueue_job(
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
            package_id=package['id'], url_type='datastore', format='csv'
        )
    records = [
        {'scientificName': 'Boops boops'},
        {'scientificName': 'Felis catus'},
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
