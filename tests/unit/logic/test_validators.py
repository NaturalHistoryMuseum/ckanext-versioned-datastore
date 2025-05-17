from unittest.mock import MagicMock, patch

import pytest
from ckan.plugins import toolkit

from ckanext.versioned_datastore.logic.validators import (
    _deduplicate,
    validate_datastore_resource_ids,
    validate_resource_ids,
)


class TestDeduplicate:
    def test_empty(self):
        assert not list(_deduplicate([]))

    def test_all_different(self):
        assert list(_deduplicate(['a', 'b', 'c'])) == ['a', 'b', 'c']

    def test_some_duplicates_and_order_preserved(self):
        assert list(_deduplicate(['a', 'd', 'a', 'b', 'a', 'c', 'b'])) == [
            'a',
            'd',
            'b',
            'c',
        ]


class TestValidateResourceIDs:
    def test_empty(self):
        assert validate_resource_ids([]) == []
        assert validate_resource_ids('') == []

    def test_not_a_list(self):
        with pytest.raises(
            toolkit.Invalid, match='Invalid list of resource ID strings'
        ):
            validate_resource_ids({})

    def test_all_valid(self):
        valid = ['a', 'c', 'x']
        check_resource_id_mock = MagicMock(side_effect=lambda r, _: r in valid)

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_resource_id',
            check_resource_id_mock,
        ):
            assert validate_resource_ids(valid) == valid

    def test_all_invalid(self):
        check_resource_id_mock = MagicMock(return_value=False)

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_resource_id',
            check_resource_id_mock,
        ):
            with pytest.raises(toolkit.Invalid, match='No resource IDs are available'):
                validate_resource_ids(['o', 'b'])

    def test_some_valid(self):
        valid = {'a', 'c', 'x'}
        check_resource_id_mock = MagicMock(side_effect=lambda r, _: r in valid)

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_resource_id',
            check_resource_id_mock,
        ):
            assert validate_resource_ids(['x', 'o', 'a', 'b', 'c']) == ['x', 'a', 'c']

    def test_some_valid_with_duplicates(self):
        valid = {'a', 'c', 'x'}
        check_resource_id_mock = MagicMock(side_effect=lambda r, _: r in valid)

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_resource_id',
            check_resource_id_mock,
        ):
            assert validate_resource_ids(['x', 'a', 'a', 'a', 'b']) == ['x', 'a']


class TestValidateDatastoreResourceIDs:
    def test_empty(self):
        assert validate_datastore_resource_ids([]) == []
        assert validate_datastore_resource_ids('') == []

    def test_not_a_list(self):
        with pytest.raises(
            toolkit.Invalid, match='Invalid list of resource ID strings'
        ):
            validate_datastore_resource_ids({})

    def test_all_valid(self):
        valid = ['a', 'c', 'x']
        check_datastore_resource_id_mock = MagicMock(
            side_effect=lambda r, _: r in valid
        )

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_datastore_resource_id',
            check_datastore_resource_id_mock,
        ):
            assert validate_datastore_resource_ids(valid) == valid

    def test_all_invalid(self):
        check_datastore_resource_id_mock = MagicMock(return_value=False)

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_datastore_resource_id',
            check_datastore_resource_id_mock,
        ):
            with pytest.raises(
                toolkit.Invalid, match='No resource IDs are datastore resources'
            ):
                validate_datastore_resource_ids(['o', 'b'])

    def test_some_valid(self):
        valid = {'a', 'c', 'x'}
        check_datastore_resource_id_mock = MagicMock(
            side_effect=lambda r, _: r in valid
        )

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_datastore_resource_id',
            check_datastore_resource_id_mock,
        ):
            assert validate_datastore_resource_ids(['x', 'o', 'a', 'b', 'c']) == [
                'x',
                'a',
                'c',
            ]

    def test_some_valid_with_duplicates(self):
        valid = {'a', 'c', 'x'}
        check_datastore_resource_id_mock = MagicMock(
            side_effect=lambda r, _: r in valid
        )

        with patch(
            'ckanext.versioned_datastore.logic.validators.check_datastore_resource_id',
            check_datastore_resource_id_mock,
        ):
            assert validate_datastore_resource_ids(['x', 'a', 'a', 'a', 'b']) == [
                'x',
                'a',
            ]
