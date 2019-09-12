import contextlib

from ckanext.versioned_datastore.lib.search import _find_version, create_search
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
