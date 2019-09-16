from ckanext.versioned_datastore.lib.ingestion.records import DatastoreRecord
from mock import MagicMock
from nose.tools import assert_equal


def test_convert_empty():
    scenarios = [
        (
            {},
            {}
        ),
        (
            {u'field1': u'beans', u'field2': [1, 2, 3, 4]},
            {u'field1': u'beans', u'field2': [1, 2, 3, 4]}
        ),
        (
            {u'field1.another': u'beans'},
            {u'field1_another': u'beans'}
        ),
        (
            {u'field1.another.more.moreagain': u'beans'},
            {u'field1_another_more_moreagain': u'beans'}
        ),
        (
            {u'': u'beans'},
            {}
        ),
        (
            {u'field': {u'field1': u'beans', u'field2.field3': False, u'a': {u'': u'beans'}}},
            {u'field': {u'field1': u'beans', u'field2_field3': False, u'a': {}}},
        ),
    ]

    for data, expected_result in scenarios:
        record = DatastoreRecord(MagicMock(), MagicMock(), data, MagicMock())
        converted = record.convert()
        assert_equal(converted, expected_result)


def test_simple_things():
    record_id = MagicMock()
    resource_id = MagicMock()
    record = DatastoreRecord(MagicMock(), record_id, MagicMock(), resource_id)
    assert_equal(record.id, record_id)
    assert_equal(record.mongo_collection, resource_id)
