from collections import defaultdict

from mock import patch


def patch_rounded_versions():
    def rounded_versions_mock(indices, target_version):
        return defaultdict(lambda: target_version)

    return patch(
        'ckanext.versioned_datastore.lib.common.SEARCH_HELPER.get_rounded_versions',
        side_effect=rounded_versions_mock,
    )
