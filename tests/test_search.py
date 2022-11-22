import contextlib

import pytest
from elasticsearch_dsl import Search
from mock import MagicMock, patch

from ckanext.versioned_datastore.lib.basic_query.search import (
    _find_version,
    create_search,
    build_search_object,
)
from ckanext.versioned_datastore.lib.datastore_utils import prefix_field


class TestFindVersion(object):
    plugins = ['versioned_datastore']

    def test_none_found(self):
        assert _find_version({}) == None

    def test_version_is_none(self):
        assert _find_version({'version': None}) == None

    def test_version_exists(self):
        assert _find_version({'version': 10}) == 10
        assert _find_version({'version': '10'}) == 10

        with pytest.raises(ValueError):
            _find_version({'version': 'aaaa'})

    def test_version_as_filter_is_none(self):
        assert _find_version({'filters': {'__version__': None}}) == None
        assert _find_version({'filters': {'__version__': [None]}}) == None
        assert _find_version({'filters': {'__version__': [None, None]}}) == None
        # only the first value is used if there is a list, even if the first value is invalid
        assert _find_version({'filters': {'__version__': [None, 10]}}) == None

    def test_version_as_filter_exists(self):
        assert _find_version({'filters': {'__version__': 10}}) == 10
        assert _find_version({'filters': {'__version__': '10'}}) == 10
        assert _find_version({'filters': {'__version__': [10]}}) == 10
        assert _find_version({'filters': {'__version__': ['10']}}) == 10
        # only the first value is used
        assert _find_version({'filters': {'__version__': ['10', None]}}) == 10

        with pytest.raises(ValueError):
            _find_version({'filters': {'__version__': 'aaaaa'}})

        with pytest.raises(ValueError):
            _find_version({'filters': {'__version__': ['aaaaa']}})

        with pytest.raises(ValueError):
            _find_version({'filters': {'__version__': ['aaaaa', None]}})

    def test_both_are_none(self):
        assert (
            _find_version({'version': None, 'filters': {'__version__': None}}) == None
        )
        assert (
            _find_version({'version': None, 'filters': {'__version__': [None]}}) == None
        )

    def test_version_takes_precedence(self):
        assert _find_version({'version': 10, 'filters': {'__version__': 12}}) == 10
        assert _find_version({'version': 10, 'filters': {'__version__': [12]}}) == 10

    def test_both_mix(self):
        assert _find_version({'version': 10, 'filters': {'__version__': None}}) == 10
        assert _find_version({'version': 10, 'filters': {'__version__': [None]}}) == 10
        assert _find_version({'version': None, 'filters': {'__version__': 12}}) == 12
        assert _find_version({'version': None, 'filters': {'__version__': [12]}}) == 12


class TestCreateSearch(object):
    @contextlib.contextmanager
    def _patch(self, target, replacement):
        with patch(
            f'ckanext.versioned_datastore.lib.basic_query.search.{target}', replacement
        ):
            yield

    def test_onward_calls(self):
        # the params
        context = MagicMock()
        data_dict = MagicMock()
        original_data_dict = MagicMock()

        # the returns of our various mocked functions
        mock_validate_return = MagicMock()
        build_mock_return = MagicMock()
        find_version_return = MagicMock()

        # the mock functions
        find_version_mock = MagicMock(return_value=find_version_return)
        build_search_object_mock = MagicMock(return_value=build_mock_return)
        mock_plugin = MagicMock(
            datastore_modify_data_dict=MagicMock(return_value=mock_validate_return),
            datastore_modify_search=MagicMock(return_value=build_mock_return),
        )
        impl_mock = MagicMock(return_value=[mock_plugin])
        with self._patch('PluginImplementations', impl_mock):
            with self._patch('_find_version', find_version_mock):
                with self._patch('build_search_object', build_search_object_mock):
                    result = create_search(context, data_dict, original_data_dict)

        # 4 values are returned
        assert len(result) == 4
        # check the values
        assert result[0] == original_data_dict
        assert result[1] == mock_validate_return
        assert result[2] == find_version_return
        assert result[3] == build_mock_return
        # check the onward calls
        assert mock_plugin.datastore_modify_data_dict.call_count == 1
        assert mock_plugin.datastore_modify_search.call_count == 1
        assert find_version_mock.call_count == 1
        assert build_search_object_mock.call_count == 1

    def test_simple_usage(self):
        # create our own schema so that we don't have to mock a resource existing to pass the
        # actual datastore search schema
        context = {'schema': {'q': [str], 'version': [int]}}
        data_dict = {'q': 'banana', 'version': 23}
        original_data_dict = {'q': 'banana', 'version': 23, 'something_else': 29}

        result = create_search(context, data_dict, original_data_dict)

        # 4 values are returned
        assert len(result) == 4
        # check the values
        assert result[0] == original_data_dict
        assert result[1] == data_dict
        assert result[2] == 23
        # we don't care in this test if the Search has been created correctly, only that we get a
        # Search object back
        assert isinstance(result[3], Search)


class TestBuildSearchObject(object):
    def _run_test(self, kwargs, expected_result, add_size=True, add_sort=True):
        if add_size:
            expected_result['size'] = 100
        if add_sort:
            expected_result['sort'] = [prefix_field('_id')]
        assert build_search_object(**kwargs).to_dict() == expected_result
        assert build_search_object(**kwargs) == Search().from_dict(expected_result)

    def test_blank(self):
        self._run_test({}, {})

    def test_q_empties(self):
        self._run_test({'q': ''}, {})
        self._run_test({'q': None}, {})
        self._run_test({'q': {}}, {})

    def test_q_simple_text(self):
        self._run_test(
            {'q': 'banana'},
            {
                'query': {
                    'match': {
                        'meta.all': {
                            'query': 'banana',
                            'operator': 'and',
                        }
                    }
                }
            },
        )
        self._run_test(
            {'q': 'a multi-word example'},
            {
                'query': {
                    'match': {
                        'meta.all': {
                            'query': 'a multi-word example',
                            'operator': 'and',
                        }
                    }
                }
            },
        )

    def test_q_dicts(self):
        self._run_test(
            {'q': {'': 'banana'}},
            {
                'query': {
                    'match': {
                        'meta.all': {
                            'query': 'banana',
                            'operator': 'and',
                        }
                    }
                }
            },
        )
        self._run_test(
            {'q': {'field1': 'banana'}},
            {
                'query': {
                    'match': {
                        prefix_field('field1'): {
                            'query': 'banana',
                            'operator': 'and',
                        }
                    }
                }
            },
        )
        self._run_test(
            {'q': {'field1': 'banana', 'field2': 'lemons'}},
            {
                'query': {
                    'bool': {
                        'must': [
                            {
                                'match': {
                                    prefix_field('field1'): {
                                        'query': 'banana',
                                        'operator': 'and',
                                    },
                                }
                            },
                            {
                                'match': {
                                    prefix_field('field2'): {
                                        'query': 'lemons',
                                        'operator': 'and',
                                    },
                                }
                            },
                        ]
                    }
                }
            },
        )

    def test_q_string_and_unicode(self):
        self._run_test(
            {'q': 'a string'},
            {
                'query': {
                    'match': {
                        'meta.all': {
                            'query': 'a string',
                            'operator': 'and',
                        }
                    }
                }
            },
        )
        self._run_test(
            {'q': 'a unicode string'},
            {
                'query': {
                    'match': {
                        'meta.all': {
                            'query': 'a unicode string',
                            'operator': 'and',
                        }
                    }
                }
            },
        )

    def test_q_non_string(self):
        self._run_test(
            {'q': 4},
            {
                'query': {
                    'match': {
                        'meta.all': {
                            'query': 4,
                            'operator': 'and',
                        }
                    }
                }
            },
        )
        self._run_test(
            {'q': 4.31932},
            {
                'query': {
                    'match': {
                        'meta.all': {
                            'query': 4.31932,
                            'operator': 'and',
                        }
                    }
                }
            },
        )

    def test_filters_empties(self):
        self._run_test({'filters': {}}, {})

    def test_filters_non_lists(self):
        self._run_test(
            {
                'filters': {
                    'field1': 'banana',
                }
            },
            {
                'query': {
                    'bool': {
                        'filter': [
                            {
                                'term': {
                                    prefix_field('field1'): 'banana',
                                }
                            }
                        ]
                    }
                }
            },
        )
        self._run_test(
            {
                'filters': {
                    'field1': 'banana',
                    'field2': 'lemons',
                }
            },
            {
                'query': {
                    'bool': {
                        'filter': [
                            {
                                'term': {
                                    prefix_field('field1'): 'banana',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field2'): 'lemons',
                                }
                            },
                        ]
                    }
                }
            },
        )

        add_geo_search_mock = MagicMock(side_effect=lambda s, v: s)
        mock_geo_value = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.search.add_geo_search',
            add_geo_search_mock,
        ):
            build_search_object(filters={'__geo__': mock_geo_value})
        assert add_geo_search_mock.call_count == 1
        search_object, filter_value = add_geo_search_mock.call_args[0]
        assert isinstance(search_object, Search)
        assert mock_geo_value == filter_value

    def test_filters_lists(self):
        self._run_test(
            {
                'filters': {
                    'field1': ['banana'],
                }
            },
            {
                'query': {
                    'bool': {
                        'filter': [
                            {
                                'term': {
                                    prefix_field('field1'): 'banana',
                                }
                            }
                        ]
                    }
                }
            },
        )
        self._run_test(
            {
                'filters': {
                    'field1': ['banana'],
                    'field2': ['lemons'],
                }
            },
            {
                'query': {
                    'bool': {
                        'filter': [
                            {
                                'term': {
                                    prefix_field('field1'): 'banana',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field2'): 'lemons',
                                }
                            },
                        ]
                    }
                }
            },
        )
        self._run_test(
            {
                'filters': {
                    'field1': ['banana', 'goat', 'funnel'],
                    'field2': ['lemons', 'chunk'],
                }
            },
            {
                'query': {
                    'bool': {
                        'filter': [
                            {
                                'term': {
                                    prefix_field('field1'): 'banana',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field1'): 'goat',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field1'): 'funnel',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field2'): 'lemons',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field2'): 'chunk',
                                }
                            },
                        ]
                    }
                }
            },
        )
        add_geo_search_mock = MagicMock(side_effect=lambda s, v: s)
        mock_geo_value = MagicMock()
        with patch(
            'ckanext.versioned_datastore.lib.basic_query.search.add_geo_search',
            add_geo_search_mock,
        ):
            build_search_object(filters={'__geo__': [mock_geo_value]})
        assert add_geo_search_mock.call_count == 1
        search_object, filter_value = add_geo_search_mock.call_args[0]
        assert isinstance(search_object, Search)
        assert mock_geo_value == filter_value

    def test_filters_mix(self):
        self._run_test(
            {
                'filters': {
                    'field1': 'banana',
                    'field2': ['lemons', 'blarp'],
                }
            },
            {
                'query': {
                    'bool': {
                        'filter': [
                            {
                                'term': {
                                    prefix_field('field1'): 'banana',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field2'): 'lemons',
                                }
                            },
                            {
                                'term': {
                                    prefix_field('field2'): 'blarp',
                                }
                            },
                        ]
                    }
                }
            },
        )

    def test_after(self):
        self._run_test({'after': 'some value'}, {'search_after': 'some value'})

    def test_offset(self):
        self._run_test({'offset': 12}, {'from': 12})
        self._run_test({'offset': '289'}, {'from': 289})

    def test_limit(self):
        self._run_test({'limit': 15}, {'size': 15}, add_size=False)
        self._run_test({'limit': '1500'}, {'size': 1500}, add_size=False)

    def test_fields(self):
        self._run_test(
            {'fields': ['field1', 'field2']},
            {'_source': ['data.field1', 'data.field2']},
        )

    def test_facets(self):
        self._run_test({'facets': []}, {})

        self._run_test(
            {'facets': ['field1']},
            {
                'aggs': {
                    'field1': {'terms': {'field': prefix_field('field1'), 'size': 10}}
                }
            },
        )

        self._run_test(
            {'facets': ['field1', 'field2', 'field3']},
            {
                'aggs': {
                    'field1': {'terms': {'field': prefix_field('field1'), 'size': 10}},
                    'field2': {'terms': {'field': prefix_field('field2'), 'size': 10}},
                    'field3': {'terms': {'field': prefix_field('field3'), 'size': 10}},
                }
            },
        )

        self._run_test(
            {
                'facets': ['field1', 'field2', 'field3'],
                'facet_limits': {
                    'field1': 20,
                    'field3': 100,
                },
            },
            {
                'aggs': {
                    'field1': {'terms': {'field': prefix_field('field1'), 'size': 20}},
                    'field2': {'terms': {'field': prefix_field('field2'), 'size': 10}},
                    'field3': {'terms': {'field': prefix_field('field3'), 'size': 100}},
                }
            },
        )

    def test_sorts(self):
        self._run_test({'sort': []}, {})
        self._run_test(
            {'sort': ['field1']},
            {'sort': [prefix_field('field1'), prefix_field('_id')]},
            add_sort=False,
        )
        self._run_test(
            {'sort': ['field1', 'field2']},
            {
                'sort': [
                    prefix_field('field1'),
                    prefix_field('field2'),
                    prefix_field('_id'),
                ]
            },
            add_sort=False,
        )
        self._run_test(
            {'sort': ['field1', '_id']},
            {'sort': [prefix_field('field1'), prefix_field('_id')]},
            add_sort=False,
        )
        self._run_test(
            {'sort': ['field1 desc', 'field2 asc']},
            {
                'sort': [
                    {prefix_field('field1'): {'order': 'desc'}},
                    prefix_field('field2'),
                    prefix_field('_id'),
                ]
            },
            add_sort=False,
        )
        self._run_test(
            {'sort': ['_id desc']},
            {'sort': [{prefix_field('_id'): {'order': 'desc'}}]},
            add_sort=False,
        )
