from ckanext.versioned_datastore.lib.search import _find_version
from ckantest.models import TestBase
from nose.tools import assert_equal, assert_raises


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
