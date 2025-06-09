import pytest
from elasticsearch_dsl import Search
from splitgill.model import Record

from ckanext.versioned_datastore.lib.query.search.query import DirectQuery
from ckanext.versioned_datastore.lib.query.search.request import SearchRequest
from ckanext.versioned_datastore.lib.utils import get_database


@pytest.mark.usefixtures('with_vds')
class TestSearchRequest:
    def test_add_param(self):
        index = 'test-1'
        req = SearchRequest(DirectQuery([index]))

        # this should fail because the index doesn't exist
        with pytest.raises(Exception):
            req.run()

        req.add_param('ignore_unavailable', True)
        # this should now succeed because of above parameter
        result = req.run()
        assert result.count == 0

    def test_indexes_normal(self):
        database = get_database('test-1')
        query = DirectQuery([database.name])
        req = SearchRequest(query)

        # no version provided, use latest
        assert req.indexes() == [database.indices.latest]

        # add a version, use wildcard
        query.version = 10
        assert req.indexes() == [database.indices.wildcard]

    def test_indexes_force_no_version(self):
        database = get_database('test-1')
        req = SearchRequest(DirectQuery([database.name]), force_no_version=True)
        assert req.indexes() == [database.indices.wildcard]

    def test_set_no_result(self):
        database = get_database('test-1')

        # add a record to the database
        database.ingest([Record.new({'a': 5})])
        database.sync()

        req = SearchRequest(DirectQuery([database.name]))
        result_1 = req.run()
        assert result_1.count == 1
        assert result_1.hits

        req.set_no_results()
        result_2 = req.run()
        assert result_2.count == 1
        assert not result_2.hits

    def test_get_safe_size(self):
        query = DirectQuery(['test-1'])
        assert SearchRequest(query).get_safe_size() == 100
        assert SearchRequest(query, size=10).get_safe_size() == 10
        assert SearchRequest(query, size=0).get_safe_size() == 0
        assert SearchRequest(query, size=-1).get_safe_size() == 0
        assert SearchRequest(query, size=1004).get_safe_size() == 1000

    def test_to_search(self):
        query = DirectQuery(['test-1'])
        req = SearchRequest(query, size=10)
        search = req.to_search()
        assert isinstance(search, Search)
        # we need this so it must be here!
        assert search._extra['track_total_hits']
        # should always be size + 1 when size is set
        assert search._extra['size'] == 11
        # todo: maybe add more tests for this?
