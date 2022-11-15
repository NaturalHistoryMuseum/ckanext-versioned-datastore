from mock import MagicMock

from ckanext.versioned_datastore.lib.importing.ingestion.records import DatastoreRecord


def test_convert_empty():
    scenarios = [
        ({}, {}),
        (
            {'field1': 'beans', 'field2': [1, 2, 3, 4]},
            {'field1': 'beans', 'field2': [1, 2, 3, 4]},
        ),
        ({'field1.another': 'beans'}, {'field1_another': 'beans'}),
        (
            {'field1.another.more.moreagain': 'beans'},
            {'field1_another_more_moreagain': 'beans'},
        ),
        ({'': 'beans'}, {}),
        (
            {'field': {'field1': 'beans', 'field2.field3': False, 'a': {'': 'beans'}}},
            {'field': {'field1': 'beans', 'field2_field3': False, 'a': {}}},
        ),
    ]

    for data, expected_result in scenarios:
        record = DatastoreRecord(MagicMock(), MagicMock(), data, MagicMock())
        converted = record.convert()
        assert converted == expected_result


def test_simple_things():
    record_id = MagicMock()
    resource_id = MagicMock()
    record = DatastoreRecord(MagicMock(), record_id, MagicMock(), resource_id)
    assert record.id == record_id
    assert record.mongo_collection == resource_id
