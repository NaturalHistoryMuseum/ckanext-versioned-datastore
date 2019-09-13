import contextlib

from ckanext.versioned_datastore.lib.search import _find_version, create_search, build_search_object
from ckanext.versioned_datastore.lib.utils import prefix_field
from ckantest.models import TestBase
from elasticsearch_dsl import Search
from mock import MagicMock, patch
from nose.tools import assert_equal, assert_raises, assert_true


class TestFindVersion(TestBase):
    plugins = [u'versioned_datastore']

    def test_none_found(self):
        assert_equal(_find_version({}), None)

    def test_version_is_none(self):
        assert_equal(_find_version({u'version': None}), None)

    def test_version_exists(self):
        assert_equal(_find_version({u'version': 10}), 10)
        assert_equal(_find_version({u'version': u'10'}), 10)

        with assert_raises(ValueError):
            _find_version({u'version': u'aaaa'})

    def test_version_as_filter_is_none(self):
        assert_equal(_find_version({u'filters': {u'__version__': None}}), None)
        assert_equal(_find_version({u'filters': {u'__version__': [None]}}), None)
        assert_equal(_find_version({u'filters': {u'__version__': [None, None]}}), None)
        # only the first value is used if there is a list, even if the first value is invalid
        assert_equal(_find_version({u'filters': {u'__version__': [None, 10]}}), None)

    def test_version_as_filter_exists(self):
        assert_equal(_find_version({u'filters': {u'__version__': 10}}), 10)
        assert_equal(_find_version({u'filters': {u'__version__': u'10'}}), 10)
        assert_equal(_find_version({u'filters': {u'__version__': [10]}}), 10)
        assert_equal(_find_version({u'filters': {u'__version__': [u'10']}}), 10)
        # only the first value is used
        assert_equal(_find_version({u'filters': {u'__version__': [u'10', None]}}), 10)

        with assert_raises(ValueError):
            _find_version({u'filters': {u'__version__': u'aaaaa'}})

        with assert_raises(ValueError):
            _find_version({u'filters': {u'__version__': [u'aaaaa']}})

        with assert_raises(ValueError):
            _find_version({u'filters': {u'__version__': [u'aaaaa', None]}})

    def test_both_are_none(self):
        assert_equal(_find_version({u'version': None, u'filters': {u'__version__': None}}), None)
        assert_equal(_find_version({u'version': None, u'filters': {u'__version__': [None]}}), None)

    def test_version_takes_precedence(self):
        assert_equal(_find_version({u'version': 10, u'filters': {u'__version__': 12}}), 10)
        assert_equal(_find_version({u'version': 10, u'filters': {u'__version__': [12]}}), 10)

    def test_both_mix(self):
        assert_equal(_find_version({u'version': 10, u'filters': {u'__version__': None}}), 10)
        assert_equal(_find_version({u'version': 10, u'filters': {u'__version__': [None]}}), 10)
        assert_equal(_find_version({u'version': None, u'filters': {u'__version__': 12}}), 12)
        assert_equal(_find_version({u'version': None, u'filters': {u'__version__': [12]}}), 12)


class TestCreateSearch(TestBase):
    plugins = [u'versioned_datastore']

    @contextlib.contextmanager
    def _patch(self, target, replacement):
        with patch(u'ckanext.versioned_datastore.lib.search.{}'.format(target), replacement):
            yield

    def test_onward_calls(self):
        # the params
        context = MagicMock()
        data_dict = MagicMock()

        # the returns of our various mocked functions
        mock_validate_return = MagicMock()
        build_mock_return = MagicMock()
        find_version_return = MagicMock()

        # the mock functions
        validate_mock = MagicMock(return_value=MagicMock())
        find_version_mock = MagicMock(return_value=find_version_return)
        build_search_object_mock = MagicMock(return_value=build_mock_return)
        mock_plugin = MagicMock(
            datastore_modify_data_dict=MagicMock(return_value=mock_validate_return),
            datastore_modify_search=MagicMock(return_value=build_mock_return),
        )
        impl_mock = MagicMock(return_value=[mock_plugin])
        with self._patch(u'validate', validate_mock):
            with self._patch(u'PluginImplementations', impl_mock):
                with self._patch(u'_find_version', find_version_mock):
                    with self._patch(u'build_search_object', build_search_object_mock):
                        result = create_search(context, data_dict)

        # 4 values are returned
        assert_equal(len(result), 4)
        # check the values
        assert_equal(result[0], data_dict)
        assert_equal(result[1], mock_validate_return)
        assert_equal(result[2], find_version_return)
        assert_equal(result[3], build_mock_return)
        # check the onward calls
        assert_equal(validate_mock.call_count, 1)
        assert_equal(mock_plugin.datastore_modify_data_dict.call_count, 1)
        assert_equal(mock_plugin.datastore_modify_search.call_count, 1)
        assert_equal(find_version_mock.call_count, 1)
        assert_equal(build_search_object_mock.call_count, 1)

    def test_simple_usage(self):
        # create our own schema so that we don't have to mock a resource existing to pass the
        # actual datastore search schema
        context = {u'schema': {u'q': [unicode], u'version': [int]}}
        data_dict = {u'q': u'banana', u'version': 23}

        result = create_search(context, data_dict)

        # 4 values are returned
        assert_equal(len(result), 4)
        # check the values
        assert_equal(result[0], data_dict)
        assert_equal(result[1], data_dict)
        assert_equal(result[2], 23)
        # we don't care in this test if the Search has been created correctly, only that we get a
        # Search object back
        assert_true(isinstance(result[3], Search))


class TestBuildSearchObject(TestBase):
    plugins = [u'versioned_datastore']

    def _run_test(self, kwargs, expected_result, add_size=True, add_sort=True):
        if add_size:
            expected_result[u'size'] = 100
        if add_sort:
            expected_result[u'sort'] = [prefix_field(u'_id')]
        assert_equal(build_search_object(**kwargs).to_dict(), expected_result)
        assert_equal(build_search_object(**kwargs), Search().from_dict(expected_result))

    def test_blank(self):
        self._run_test({}, {})

    def test_q_empties(self):
        self._run_test({u'q': u''}, {})
        self._run_test({u'q': None}, {})
        self._run_test({u'q': {}}, {})

    def test_q_simple_text(self):
        self._run_test(
            {
                u'q': u'banana'
            },
            {
                u'query': {
                    u'match': {
                        u'meta.all': {
                            u'query': u'banana',
                            u'operator': u'and',
                        }
                    }
                }
            }
        )
        self._run_test(
            {
                u'q': u'a multi-word example'
            },
            {
                u'query': {
                    u'match': {
                        u'meta.all': {
                            u'query': u'a multi-word example',
                            u'operator': u'and',
                        }
                    }
                }
            }
        )

    def test_q_dicts(self):
        self._run_test(
            {
                u'q': {
                    u'': u'banana'
                }
            },
            {
                u'query': {
                    u'match': {
                        u'meta.all': {
                            u'query': u'banana',
                            u'operator': u'and',
                        }
                    }
                }
            }
        )
        self._run_test(
            {
                u'q': {
                    u'field1': u'banana'
                }
            },
            {
                u'query': {
                    u'match': {
                        prefix_field(u'field1'): {
                            u'query': u'banana',
                            u'operator': u'and',
                        }
                    }
                }
            }
        )
        self._run_test(
            {
                u'q': {
                    u'field1': u'banana',
                    u'field2': u'lemons'
                }
            },
            {
                u'query': {
                    u'bool': {
                        u'must': [
                            {
                                u'match': {
                                    prefix_field(u'field1'): {
                                        u'query': u'banana',
                                        u'operator': u'and',
                                    },
                                }
                            },
                            {
                                u'match': {
                                    prefix_field(u'field2'): {
                                        u'query': u'lemons',
                                        u'operator': u'and',
                                    },
                                }
                            },
                        ]
                    }
                }
            }
        )

    def test_q_string_and_unicode(self):
        self._run_test(
            {
                u'q': 'a string'
            },
            {
                u'query': {
                    u'match': {
                        u'meta.all': {
                            u'query': 'a string',
                            u'operator': u'and',
                        }
                    }
                }
            }
        )
        self._run_test(
            {
                u'q': u'a unicode string'
            },
            {
                u'query': {
                    u'match': {
                        u'meta.all': {
                            u'query': u'a unicode string',
                            u'operator': u'and',
                        }
                    }
                }
            }
        )

    def test_q_non_string(self):
        self._run_test(
            {
                u'q': 4
            },
            {
                u'query': {
                    u'match': {
                        u'meta.all': {
                            u'query': 4,
                            u'operator': u'and',
                        }
                    }
                }
            }
        )
        self._run_test(
            {
                u'q': 4.31932
            },
            {
                u'query': {
                    u'match': {
                        u'meta.all': {
                            u'query': 4.31932,
                            u'operator': u'and',
                        }
                    }
                }
            }
        )

    def test_filters_empties(self):
        self._run_test({u'filters': {}}, {})

    def test_filters_non_lists(self):
        self._run_test(
            {
                u'filters': {
                    u'field1': u'banana',
                }
            },
            {
                u'query': {
                    u'bool': {
                        u'filter': [
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'banana',
                                }
                            }
                        ]
                    }
                }
            }
        )
        self._run_test(
            {
                u'filters': {
                    u'field1': u'banana',
                    u'field2': u'lemons',
                }
            },
            {
                u'query': {
                    u'bool': {
                        u'filter': [
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'banana',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field2'): u'lemons',
                                }
                            }
                        ]
                    }
                }
            }
        )

        add_geo_search_mock = MagicMock(side_effect=lambda s, v: s)
        mock_geo_value = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.search.add_geo_search', add_geo_search_mock):
            build_search_object(filters={u'__geo__': mock_geo_value})
        assert_equal(add_geo_search_mock.call_count, 1)
        search_object, filter_value = add_geo_search_mock.call_args[0]
        assert_true(isinstance(search_object, Search))
        assert_equal(mock_geo_value, filter_value)

    def test_filters_lists(self):
        self._run_test(
            {
                u'filters': {
                    u'field1': [u'banana'],
                }
            },
            {
                u'query': {
                    u'bool': {
                        u'filter': [
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'banana',
                                }
                            }
                        ]
                    }
                }
            }
        )
        self._run_test(
            {
                u'filters': {
                    u'field1': [u'banana'],
                    u'field2': [u'lemons'],
                }
            },
            {
                u'query': {
                    u'bool': {
                        u'filter': [
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'banana',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field2'): u'lemons',
                                }
                            }
                        ]
                    }
                }
            }
        )
        self._run_test(
            {
                u'filters': {
                    u'field1': [u'banana', u'goat', u'funnel'],
                    u'field2': [u'lemons', u'chunk'],
                }
            },
            {
                u'query': {
                    u'bool': {
                        u'filter': [
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'banana',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'goat',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'funnel',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field2'): u'lemons',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field2'): u'chunk',
                                }
                            }
                        ]
                    }
                }
            }
        )
        add_geo_search_mock = MagicMock(side_effect=lambda s, v: s)
        mock_geo_value = MagicMock()
        with patch(u'ckanext.versioned_datastore.lib.search.add_geo_search', add_geo_search_mock):
            build_search_object(filters={u'__geo__': [mock_geo_value]})
        assert_equal(add_geo_search_mock.call_count, 1)
        search_object, filter_value = add_geo_search_mock.call_args[0]
        assert_true(isinstance(search_object, Search))
        assert_equal(mock_geo_value, filter_value)

    def test_filters_mix(self):
        self._run_test(
            {
                u'filters': {
                    u'field1': u'banana',
                    u'field2': [u'lemons', u'blarp'],
                }
            },
            {
                u'query': {
                    u'bool': {
                        u'filter': [
                            {
                                u'term': {
                                    prefix_field(u'field1'): u'banana',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field2'): u'lemons',
                                }
                            },
                            {
                                u'term': {
                                    prefix_field(u'field2'): u'blarp',
                                }
                            }
                        ]
                    }
                }
            }
        )

    def test_after(self):
        self._run_test({u'after': u'some value'}, {u'search_after': u'some value'})

    def test_offset(self):
        self._run_test({u'offset': 12}, {u'from': 12})
        self._run_test({u'offset': u'289'}, {u'from': 289})

    def test_limit(self):
        self._run_test({u'limit': 15}, {u'size': 15}, add_size=False)
        self._run_test({u'limit': u'1500'}, {u'size': 1500}, add_size=False)

    def test_fields(self):
        self._run_test({u'fields': [u'field1', u'field2']}, {u'_source': [u'data.field1',
                                                                          u'data.field2']})

    def test_facets(self):
        self._run_test({u'facets': []}, {})

        self._run_test(
            {
                u'facets': [u'field1']
            },
            {
                u'aggs': {
                    u'field1': {
                        u'terms': {
                            u'field': prefix_field(u'field1'),
                            u'size': 10
                        }
                    }
                }
            }
        )

        self._run_test(
            {
                u'facets': [u'field1', u'field2', u'field3']
            },
            {
                u'aggs': {
                    u'field1': {
                        u'terms': {
                            u'field': prefix_field(u'field1'),
                            u'size': 10
                        }
                    },
                    u'field2': {
                        u'terms': {
                            u'field': prefix_field(u'field2'),
                            u'size': 10
                        }
                    },
                    u'field3': {
                        u'terms': {
                            u'field': prefix_field(u'field3'),
                            u'size': 10
                        }
                    }
                }
            }
        )

        self._run_test(
            {
                u'facets': [u'field1', u'field2', u'field3'],
                u'facet_limits': {
                    u'field1': 20,
                    u'field3': 100,
                }
            },
            {
                u'aggs': {
                    u'field1': {
                        u'terms': {
                            u'field': prefix_field(u'field1'),
                            u'size': 20
                        }
                    },
                    u'field2': {
                        u'terms': {
                            u'field': prefix_field(u'field2'),
                            u'size': 10
                        }
                    },
                    u'field3': {
                        u'terms': {
                            u'field': prefix_field(u'field3'),
                            u'size': 100
                        }
                    }
                }
            }
        )

    def test_sorts(self):
        self._run_test({u'sort': []}, {})
        self._run_test(
            {
                u'sort': [u'field1']
            },
            {
                u'sort': [prefix_field(u'field1'), prefix_field(u'_id')]
            },
            add_sort=False
        )
        self._run_test(
            {
                u'sort': [u'field1', u'field2']
            },
            {
                u'sort': [prefix_field(u'field1'), prefix_field(u'field2'), prefix_field(u'_id')]
            },
            add_sort=False
        )
        self._run_test(
            {
                u'sort': [u'field1', u'_id']
            },
            {
                u'sort': [prefix_field(u'field1'), prefix_field(u'_id')]
            },
            add_sort=False
        )
        self._run_test(
            {
                u'sort': [u'field1 desc', u'field2 asc']
            },
            {
                u'sort': [
                    {
                        prefix_field(u'field1'): {u'order': u'desc'}
                    },
                    prefix_field(u'field2'),
                    prefix_field(u'_id')
                ]
            },
            add_sort=False
        )
        self._run_test(
            {
                u'sort': [u'_id desc']
            },
            {
                u'sort': [{
                    prefix_field(u'_id'): {u'order': u'desc'}
                }]
            },
            add_sort=False
        )
