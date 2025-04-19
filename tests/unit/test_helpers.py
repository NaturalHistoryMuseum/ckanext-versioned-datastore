from unittest.mock import MagicMock, patch

from ckanext.versioned_datastore.helpers import (
    get_available_formats,
    get_human_duration,
    get_stat_activity_class,
    get_stat_icon,
    get_stat_title,
    is_duplicate_ingestion,
)
from ckanext.versioned_datastore.lib.common import ALL_FORMATS
from ckanext.versioned_datastore.lib.importing.readers import ReaderNotFound
from ckanext.versioned_datastore.lib.importing.tasks import get_dupe_message
from ckanext.versioned_datastore.model.stats import ALL_TYPES, INDEX, INGEST, PREP


class TestHelpers(object):
    def test_is_duplicate_ingestion_works(self):
        stat = MagicMock(error=get_dupe_message('notarealhash'))
        assert is_duplicate_ingestion(stat)

    def test_is_duplicate_ingestion_avoids_other_errors(self):
        reader_not_found_exception = ReaderNotFound('test')
        # just the message should fail
        stat = MagicMock(error=str(reader_not_found_exception))
        assert not is_duplicate_ingestion(stat)

    def test_get_human_duration(self):
        scenarios = [
            # seconds
            (10.381, '10.38 seconds'),
            (10, '10.00 seconds'),
            (0, '0.00 seconds'),
            (2.111111111111328042384, '2.11 seconds'),
            (2.3, '2.30 seconds'),
            (10.385, '10.38 seconds'),
            # minutes
            (60, '1 minutes'),
            (61, '1 minutes'),
            (190, '3 minutes'),
            (280.30290, '5 minutes'),
            # hours
            (3600, '1 hours'),
            (3600.3289, '1 hours'),
            (11900, '3 hours'),
            (60 * 60 * 24 * 365 * 3, '26280 hours'),
        ]
        for duration, expected_output in scenarios:
            stat = MagicMock(duration=duration)
            assert get_human_duration(stat) == expected_output

    def test_get_stat_icon(self):
        # in progress stats are always pulsing spinners regardless of their type
        assert get_stat_icon(MagicMock(in_progress=True)) == 'fa-spinner fa-pulse'

        # if there's a non-duplicate error, we use exclamation
        with patch(
            'ckanext.versioned_datastore.helpers.is_duplicate_ingestion',
            MagicMock(return_value=False),
        ):
            assert (
                get_stat_icon(MagicMock(in_progress=False, error=MagicMock()))
                == 'fa-exclamation'
            )

        # if there's a duplicate error, we use copy
        with patch(
            'ckanext.versioned_datastore.helpers.is_duplicate_ingestion',
            MagicMock(return_value=True),
        ):
            assert (
                get_stat_icon(MagicMock(in_progress=False, error=MagicMock()))
                == 'fa-copy'
            )

        # now check the types
        assert (
            get_stat_icon(MagicMock(in_progress=False, error=None, type=INGEST))
            == 'fa-tasks'
        )
        assert (
            get_stat_icon(MagicMock(in_progress=False, error=None, type=INDEX))
            == 'fa-search'
        )
        assert (
            get_stat_icon(MagicMock(in_progress=False, error=None, type=PREP))
            == 'fa-cogs'
        )

        # check that no types are missing icons
        for stat_type in ALL_TYPES:
            assert (
                get_stat_icon(MagicMock(in_progress=False, error=None, type=stat_type))
                != 'fa-check'
            )

        # anything else gets a check to avoid erroring
        assert (
            get_stat_icon(MagicMock(in_progress=False, error=None, type='banana'))
            == 'fa-check'
        )

    def test_get_stat_activity_class(self):
        # in progress stats always get the in_progress class
        assert get_stat_activity_class(MagicMock(in_progress=True)) == 'in_progress'

        # if there's a non-duplicate error, we use failure
        with patch(
            'ckanext.versioned_datastore.helpers.is_duplicate_ingestion',
            MagicMock(return_value=False),
        ):
            assert (
                get_stat_activity_class(MagicMock(in_progress=False, error=MagicMock()))
                == 'failure'
            )

        # if there's a duplicate error, we use duplicate
        with patch(
            'ckanext.versioned_datastore.helpers.is_duplicate_ingestion',
            MagicMock(return_value=True),
        ):
            assert (
                get_stat_activity_class(MagicMock(in_progress=False, error=MagicMock()))
                == 'duplicate'
            )

        # now check the types. For these we just return the actual type value as the return value
        for stat_type in ALL_TYPES + [MagicMock()]:
            assert (
                get_stat_activity_class(
                    MagicMock(in_progress=False, error=None, type=stat_type)
                )
                == stat_type
            )

    def test_get_stat_title(self):
        # just check that for the types we know about we don't get back the default
        for stat_type in ALL_TYPES:
            assert get_stat_title(MagicMock(type=stat_type)) != stat_type

        fake_type = MagicMock()
        assert get_stat_title(MagicMock(type=fake_type)) == fake_type

    def test_get_available_formats(self):
        formats = get_available_formats()
        assert isinstance(formats, set)
        for f in ALL_FORMATS:
            assert f in formats
