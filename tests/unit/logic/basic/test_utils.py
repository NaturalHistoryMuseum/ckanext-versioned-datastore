from collections import Counter

import pytest
from splitgill.indexing.fields import DataField, ParsedField, ParsedType
from splitgill.model import Record

from ckanext.versioned_datastore.lib.importing.options import (
    create_default_options_builder,
)
from ckanext.versioned_datastore.lib.query.search.query import BasicQuery
from ckanext.versioned_datastore.lib.query.search.sort import Sort
from ckanext.versioned_datastore.lib.utils import get_database
from ckanext.versioned_datastore.logic.basic.utils import (
    find_version,
    format_facets,
    get_fields,
    infer_type,
    make_request,
)


def test_format_facets():
    # first, check it can deal with an empty aggregation result
    assert format_facets({}) == {}

    facets = format_facets(
        {
            'facet1': {
                'sum_other_doc_count': 901,
                'doc_count_error_upper_bound': 12,
                'buckets': [
                    {'key': 'value1', 'doc_count': 43},
                    {'key': 'value2', 'doc_count': 243},
                    {'key': 'value3', 'doc_count': 543},
                    {'key': 'value4', 'doc_count': 143},
                    {'key': 'value5', 'doc_count': 743},
                ],
            },
            'facet2': {
                'sum_other_doc_count': 0,
                'doc_count_error_upper_bound': 0,
                'buckets': [
                    {'key': 'value1', 'doc_count': 6},
                ],
            },
        }
    )

    assert len(facets) == 2
    assert facets['facet1']['details']['sum_other_doc_count'] == 901
    assert facets['facet1']['details']['doc_count_error_upper_bound'] == 12
    assert len(facets['facet1']['values']) == 5
    assert facets['facet1']['values']['value1'] == 43
    assert facets['facet1']['values']['value2'] == 243
    assert facets['facet1']['values']['value3'] == 543
    assert facets['facet1']['values']['value4'] == 143
    assert facets['facet1']['values']['value5'] == 743

    assert facets['facet2']['details']['sum_other_doc_count'] == 0
    assert facets['facet2']['details']['doc_count_error_upper_bound'] == 0
    assert len(facets['facet2']['values']) == 1
    assert facets['facet2']['values']['value1'] == 6


@pytest.mark.filterwarnings('ignore::sqlalchemy.exc.SADeprecationWarning')
@pytest.mark.usefixtures('with_vds')
def test_get_fields():
    database = get_database('test')
    database.update_options(create_default_options_builder().build(), commit=False)
    database.ingest(
        [Record.new({'field1': 'egg', 'field2': 52, 'field3': [1, 2, 3]})], commit=True
    )
    database.sync()

    fields = get_fields('test')
    assert fields == [
        {'id': '_id', 'type': 'string'},
        {'id': 'field1', 'type': 'string', 'sortable': True},
        {'id': 'field2', 'type': 'number', 'sortable': True},
        {'id': 'field3', 'type': 'array', 'sortable': False},
    ]


class TestInferType:
    def test_non_parsed(self):
        df = DataField('test')
        pfs = {}
        assert infer_type(df, pfs) == 'object'

    def test_threshold(self):
        df = DataField('test')
        pf = ParsedField('test', count=10, type_counts=Counter({ParsedType.NUMBER: 5}))
        pfs = {'test': pf}
        assert infer_type(df, pfs, threshold=0.9) == 'string'
        assert infer_type(df, pfs, threshold=0.3) == 'number'
        assert infer_type(df, pfs, threshold=0.5) == 'number'

    def test_order_of_preference(self):
        df = DataField('test')
        pf = ParsedField('test', count=10, type_counts=Counter())
        pfs = {'test': pf}

        assert infer_type(df, pfs) == 'string'

        pf.type_counts[ParsedType.NUMBER] = 10
        assert infer_type(df, pfs) == 'number'

        pf.type_counts[ParsedType.BOOLEAN] = 10
        assert infer_type(df, pfs) == 'boolean'

        pf.type_counts[ParsedType.DATE] = 10
        assert infer_type(df, pfs) == 'date'


class TestFindVersion:
    plugins = ['versioned_datastore']

    def test_none_found(self):
        assert find_version({}) is None

    def test_version_is_none(self):
        assert find_version({'version': None}) is None

    def test_version_exists(self):
        assert find_version({'version': 10}) == 10
        assert find_version({'version': '10'}) == 10

        with pytest.raises(ValueError):
            find_version({'version': 'aaaa'})

    def test_version_as_filter_is_none(self):
        assert find_version({'filters': {'__version__': None}}) is None
        assert find_version({'filters': {'__version__': [None]}}) is None
        assert find_version({'filters': {'__version__': [None, None]}}) is None
        # only the first value is used if there is a list, even if the first value is
        # invalid
        assert find_version({'filters': {'__version__': [None, 10]}}) is None

    def test_version_as_filter_exists(self):
        assert find_version({'filters': {'__version__': 10}}) == 10
        assert find_version({'filters': {'__version__': '10'}}) == 10
        assert find_version({'filters': {'__version__': [10]}}) == 10
        assert find_version({'filters': {'__version__': ['10']}}) == 10
        # only the first value is used
        assert find_version({'filters': {'__version__': ['10', None]}}) == 10

        with pytest.raises(ValueError):
            find_version({'filters': {'__version__': 'aaaaa'}})

        with pytest.raises(ValueError):
            find_version({'filters': {'__version__': ['aaaaa']}})

        with pytest.raises(ValueError):
            find_version({'filters': {'__version__': ['aaaaa', None]}})

    def test_both_are_none(self):
        assert find_version({'version': None, 'filters': {'__version__': None}}) is None
        assert (
            find_version({'version': None, 'filters': {'__version__': [None]}}) is None
        )

    def test_version_takes_precedence(self):
        assert find_version({'version': 10, 'filters': {'__version__': 12}}) == 10
        assert find_version({'version': 10, 'filters': {'__version__': [12]}}) == 10

    def test_both_mix(self):
        assert find_version({'version': 10, 'filters': {'__version__': None}}) == 10
        assert find_version({'version': 10, 'filters': {'__version__': [None]}}) == 10
        assert find_version({'version': None, 'filters': {'__version__': 12}}) == 12
        assert find_version({'version': None, 'filters': {'__version__': [12]}}) == 12


class TestMakeRequest:
    def test_empty(self):
        request = make_request({'resource_id': 'test'})
        assert request.data_dict == {'resource_id': 'test'}
        assert request.query == BasicQuery('test', None, None, None)
        assert request.size is None
        assert request.offset is None
        assert request.after is None
        assert request.sorts == []
        assert request.fields == []
        assert request.aggs == {}

    def test_with_query(self):
        request = make_request({'resource_id': 'test', 'q': 'aves'})
        assert request.data_dict == {'resource_id': 'test', 'q': 'aves'}
        assert request.query == BasicQuery('test', None, 'aves', None)

    def test_with_filters(self):
        dd = {'resource_id': 'test', 'filters': {'class': 'aves'}}
        request = make_request(dd)
        assert request.data_dict == dd
        assert request.query == BasicQuery('test', None, None, dd['filters'])

    def test_with_options(self):
        dd = {
            'resource_id': 'test',
            'limit': 4,
            'offset': 9,
            'after': 'someaftervalue',
            'sort': ['field1 asc', 'field2 desc'],
            'fields': ['field4', 'field5'],
        }
        request = make_request(dd)
        assert request.data_dict == dd
        assert request.query == BasicQuery('test', None, None, None)
        assert request.size == 4
        assert request.offset == 9
        assert request.after == 'someaftervalue'
        assert request.sorts == [Sort('field1'), Sort('field2', False)]
        assert request.fields == ['field4', 'field5']

    # todo: add facet test
