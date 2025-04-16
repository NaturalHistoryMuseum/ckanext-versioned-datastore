import os
import shutil
from contextlib import suppress

import pytest
from ckan import plugins
from ckan.lib.search import clear_all
from ckan.plugins import toolkit
from ckan.tests import factories, helpers
from mock import patch

from ckanext.versioned_datastore.lib.common import DATASTORE_ONLY_RESOURCE
from ckanext.versioned_datastore.lib.utils import sg_client
from ckanext.versioned_datastore.model import details, downloads, slugs, stats
from tests.helpers import data as test_data
from tests.helpers import utils


def reset_db():
    # reset the ckan tables
    helpers.reset_db()
    # sort out the vds tables
    tables = [
        stats.import_stats_table,
        slugs.datastore_slugs_table,
        slugs.navigational_slugs_table,
        details.datastore_resource_details_table,
        downloads.datastore_downloads_core_files_table,
        downloads.datastore_downloads_derivative_files_table,
        downloads.datastore_downloads_requests_table,
    ]
    for table in reversed(tables):
        if table.exists():
            table.delete()
    for table in tables:
        if not table.exists():
            table.create()


def reset_datastore():
    sg = sg_client()
    # first clear mongo
    database_names = sg.mongo.list_database_names()
    for name in database_names:
        # the list_database_names function gives us back names like "admin" which we
        # can't drop, so catch any exceptions to avoid silly errors but provide maximum
        # clean up
        with suppress(Exception):
            sg.mongo.drop_database(name)

    # then clear elasticsearch
    sg.elasticsearch.indices.delete(index='*')
    index_templates = sg.elasticsearch.indices.get_index_template(name='*')
    for index_template in index_templates['index_templates']:
        with suppress(Exception):
            sg.elasticsearch.indices.delete_index_template(name=index_template['name'])


def reset_downloads():
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)


@pytest.fixture
def with_vds():
    if not plugins.plugin_loaded('versioned_datastore'):
        plugins.load('versioned_datastore')
    reset_db()
    clear_all()
    reset_datastore()
    reset_downloads()

    yield

    reset_downloads()
    reset_datastore()
    clear_all()
    reset_db()
    if plugins.plugin_loaded('versioned_datastore'):
        plugins.unload('versioned_datastore')


@pytest.fixture
@pytest.mark.usefixtures('with_vds')
def with_vds_resource():
    """
    Adds some test data to the datastore.
    """
    # because user_show is called in vds_data_add
    user = factories.Sysadmin()
    package = factories.Dataset()
    resource_one = factories.Resource(
        package_id=package['id'],
        url_type='datastore',
        url=DATASTORE_ONLY_RESOURCE,
    )
    resource_two = factories.Resource(
        package_id=package['id'],
        url_type='datastore',
        url=DATASTORE_ONLY_RESOURCE,
    )

    def queue_mock(func, queue, title, **kwargs):
        return utils.sync_enqueue_job_thread(func, queue=queue, title=title)

    with patch('ckanext.versioned_datastore.lib.tasks.toolkit.enqueue_job', queue_mock):
        helpers.call_action(
            'vds_data_add',
            context={'user': user['id']},
            resource_id=resource_one['id'],
            replace=True,
            records=test_data.records,
        )
        helpers.call_action(
            'vds_data_add',
            context={'user': user['id']},
            resource_id=resource_two['id'],
            replace=True,
            records=test_data.records_addtl,
        )

    # test method here
    yield resource_one, resource_two
