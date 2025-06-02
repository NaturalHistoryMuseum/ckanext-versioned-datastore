import pytest

from ckanext.versioned_datastore.lib.query.search.query import DirectQuery
from ckanext.versioned_datastore.lib.query.search.request import SearchRequest


class TestSearchRequest:
    def test_add_param(self, with_vds):
        index = 'test-1'
        req = SearchRequest(DirectQuery([index]))

        # this should fail because the index doesn't exist
        with pytest.raises(Exception):
            req.run()

        req.add_param('ignore_unavailable', True)
        # this should now succeed because of above parameter
        result = req.run()
        assert result.count == 0
