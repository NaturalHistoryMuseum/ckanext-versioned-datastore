from traceback import format_exception_only

from nose.tools import assert_equal, assert_true, assert_false
from ckanext.versioned_datastore.helpers import is_duplicate_ingestion, get_human_duration
from ckanext.versioned_datastore.lib.ingestion.exceptions import DuplicateDataSource, \
    UnsupportedDataSource
from ckantest.models import TestBase
from mock import MagicMock


class TestHelpers(TestBase):
    plugins = [u'versioned_datastore']

    def test_is_duplicate_ingestion(self):
        dup_exception = DuplicateDataSource(u'some_file_hash')

        # should be able to match on just the message
        stat1 = MagicMock(error=dup_exception.message)
        assert_true(is_duplicate_ingestion(stat1))

        # but also the final line of the actual stack output
        stat2 = MagicMock(error=unicode(
            format_exception_only(DuplicateDataSource, dup_exception)[-1].strip()
        ))
        assert_true(is_duplicate_ingestion(stat2))

        # it shouldn't match other things, for example a UnsupportedDataSource exception
        non_dup_exception = UnsupportedDataSource(u'csv')

        # just the message should fail
        stat3 = MagicMock(error=non_dup_exception.message)
        assert_false(is_duplicate_ingestion(stat3))

        # and so should the final line of the actual stack output
        stat4 = MagicMock(error=unicode(
            format_exception_only(UnsupportedDataSource, non_dup_exception)[-1].strip()
        ))
        assert_false(is_duplicate_ingestion(stat4))

    def test_get_human_duration(self):
        scenarios = [
            # seconds
            (10.381, u'10.38 seconds'),
            (10, u'10.00 seconds'),
            (0, u'0.00 seconds'),
            (2.111111111111328042384, u'2.11 seconds'),
            (2.3, u'2.30 seconds'),
            (10.385, u'10.38 seconds'),

            # minutes
            (60, u'1 minutes'),
            (61, u'1 minutes'),
            (190, u'3 minutes'),
            (280.30290, u'5 minutes'),

            # hours
            (3600, u'1 hours'),
            (3600.3289, u'1 hours'),
            (11900, u'3 hours'),
            (60 * 60 * 24 * 365 * 3, u'26280 hours'),
        ]
        for duration, expected_output in scenarios:
            stat = MagicMock(duration=duration)
            assert_equal(get_human_duration(stat), expected_output)
