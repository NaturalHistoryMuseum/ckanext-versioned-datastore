import random

import pytest

from ckanext.versioned_datastore.lib.datastore_utils import UpsertTooManyRecordsException
from ckanext.versioned_datastore.logic.actions.crud import validate_records_size


@pytest.fixture
def records():
    records = []
    for i in range(100):
        records.append({
            'field1': random.choice(('banana', 'lemon', 'lime', 'apple')),
            'field2': random.choice(
                ('goat', 'whale', 'llama', 'dinosaur', 'bee', 'kestral', 'donkey')),
            'field3': random.choice((4, 7)),
            'field4': random.sample(range(5000), 4),
            'field5': random.choice(range(100000)),
        })
    return records


def test_validate_records_bytes(records):
    # this shouldn't raise an exception
    validate_records_size(records, limit=100, byte_limit=100000000)

    # this should raise an exception due to the bytes length
    with pytest.raises(UpsertTooManyRecordsException):
        validate_records_size(records, limit=100, byte_limit=10)

    # this should raise an exception due to the records length
    with pytest.raises(UpsertTooManyRecordsException):
        validate_records_size(records, limit=10, byte_limit=100000000)

    # this should raise an exception due to either of the limits
    with pytest.raises(UpsertTooManyRecordsException):
        validate_records_size(records, limit=0, byte_limit=0)
