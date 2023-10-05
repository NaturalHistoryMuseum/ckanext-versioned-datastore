import pytest
from ckan.model import Session
from mock import patch, MagicMock

from ckanext.versioned_datastore.lib.downloads.download import DownloadRunManager
from ckanext.versioned_datastore.logic.actions.meta.arg_objects import (
    QueryArgs,
    DerivativeArgs,
    ServerArgs,
    NotifierArgs,
)
from ckanext.versioned_datastore.model.downloads import (
    DownloadRequest,
    CoreFileRecord,
    DerivativeFileRecord,
)
from tests.helpers import patches


@pytest.mark.ckan_config('ckan.plugins', 'versioned_datastore')
@pytest.mark.usefixtures('with_plugins', 'with_versioned_datastore_tables')
class TestDownloadRunManager:
    def test_create_run_manager(self):
        query_args = QueryArgs(query={}, query_version='v1.0.0')
        derivative_args = DerivativeArgs(format='csv')
        server_args = ServerArgs(**ServerArgs.defaults)
        notifier_args = NotifierArgs(type='none')

        # database should be empty before we start
        assert Session.query(CoreFileRecord).count() == 0
        assert Session.query(DerivativeFileRecord).count() == 0
        assert Session.query(DownloadRequest).count() == 0

        with patches.get_available_resources(), patches.rounded_versions():
            run_manager = DownloadRunManager(
                query_args, derivative_args, server_args, notifier_args
            )

        core_files = Session.query(CoreFileRecord).all()
        derivatives = Session.query(DerivativeFileRecord).all()
        request_records = Session.query(DownloadRequest).all()
        assert len(core_files) == 1
        assert len(derivatives) == 1
        assert len(request_records) == 1
        assert request_records[0].core_id == core_files[0].id
        assert request_records[0].derivative_id == derivatives[0].id
        assert derivatives[0].core_id == core_files[0].id

        assert run_manager.query.query == {}
        assert run_manager.query.query_version == 'v1.0.0'

        assert run_manager.derivative_options.format == 'csv'

        assert run_manager.notifier.name == 'none'

    def test_download_before_run(self):
        mock_plugin = MockPlugin()
        query_args = QueryArgs(query={}, query_version='v1.0.0')
        derivative_args = DerivativeArgs(format='csv')
        server_args = ServerArgs(**ServerArgs.defaults)
        notifier_args = NotifierArgs(type='none')

        with patch(
            'ckanext.versioned_datastore.lib.downloads.download.PluginImplementations',
            return_value=[mock_plugin],
        ), patches.get_available_resources(), patches.rounded_versions():
            run_manager = DownloadRunManager(
                query_args, derivative_args, server_args, notifier_args
            )

        assert run_manager.derivative_options.format == 'dwc'


class MockPlugin:
    def download_before_run(
        self, query_args, derivative_args, server_args, notifier_args
    ):
        derivative_args.format = 'dwc'
        return query_args, derivative_args, server_args, notifier_args

    def download_after_init(self, request):
        return
