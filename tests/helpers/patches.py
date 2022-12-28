from collections import defaultdict, namedtuple

from mock import patch, MagicMock

from tests.helpers.utils import sync_enqueue_job


def rounded_versions():
    """
    Patches get_rounded_versions to return the target version regardless of input.

    :return:
    """

    def rounded_versions_mock(indices, target_version):
        return defaultdict(lambda: target_version)

    return patch(
        'ckanext.versioned_datastore.lib.common.SEARCH_HELPER.get_rounded_versions',
        side_effect=rounded_versions_mock,
    )


def elasticsearch_scan():
    """
    Patches elasticsearch_dsl.Search.scan to return a test resource.
    """
    MockHit = namedtuple('MockHit', ['name', 'data'])
    return patch(
        'ckanext.versioned_datastore.lib.query.utils.Search.scan',
        return_value=[
            MockHit(
                name='test-resource-id',
                data=MagicMock(
                    **{'to_dict.return_value': {'scientificName': 'Boops boops'}}
                ),
            )
        ],
    )


def enqueue_job():
    return patch('ckan.plugins.toolkit.enqueue_job', side_effect=sync_enqueue_job)


def elasticsearch_client():
    return patch(
        'ckanext.versioned_datastore.lib.downloads.download.get_elasticsearch_client',
        side_effect=MagicMock(),
    )