import random

import pytest

from ckanext.versioned_datastore.lib.datastore_utils import UpsertTooManyRecordsException
from ckanext.versioned_datastore.logic.actions.crud import validate_records_size


def test_validate_records_size():
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

    # this shouldn't raise an exception
    validate_records_size(records, limit=100000000)

    # this should raise an exception
    with pytest.raises(UpsertTooManyRecordsException):
        validate_records_size(records, limit=10)
