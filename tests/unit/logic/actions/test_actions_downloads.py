from unittest.mock import patch

import pytest

from ckanext.versioned_datastore.logic.download.action import vds_download_queue
from ckanext.versioned_datastore.logic.download.arg_objects import (
    DerivativeArgs,
    NotifierArgs,
    QueryArgs,
)
from tests.helpers import patches


class TestQueueDownload:
    @pytest.mark.usefixtures('with_vds')
    def test_queue_direct_call(self):
        # there is a very similar test in test_downloads.py that calls this via the API
        # instead
        resource_ids = ['test-resource-id']

        with patch('ckan.plugins.toolkit.enqueue_job') as enqueue_mock:
            with patches.rounded_versions():
                with patches.get_available_resources(resource_ids):
                    with patches.url_for():
                        vds_download_queue(
                            {},
                            QueryArgs(),
                            DerivativeArgs(format='csv'),
                            notifier=NotifierArgs(type='none'),
                        )
                        assert enqueue_mock.call_count == 1
