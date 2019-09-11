from traceback import format_exception_only

import nose
from ckanext.versioned_datastore.helpers import is_duplicate_ingestion
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
        nose.tools.assert_true(is_duplicate_ingestion(stat1))

        # but also the final line of the actual stack output
        stat2 = MagicMock(error=unicode(
            format_exception_only(DuplicateDataSource, dup_exception)[-1].strip()
        ))
        nose.tools.assert_true(is_duplicate_ingestion(stat2))

        # it shouldn't match other things, for example a UnsupportedDataSource exception
        non_dup_exception = UnsupportedDataSource(u'csv')

        # just the message should fail
        stat3 = MagicMock(error=non_dup_exception.message)
        nose.tools.assert_false(is_duplicate_ingestion(stat3))

        # and so should the final line of the actual stack output
        stat4 = MagicMock(error=unicode(
            format_exception_only(UnsupportedDataSource, non_dup_exception)[-1].strip()
        ))
        nose.tools.assert_false(is_duplicate_ingestion(stat4))
