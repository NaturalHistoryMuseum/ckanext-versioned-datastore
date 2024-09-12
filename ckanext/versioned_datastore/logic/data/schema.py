from ckantools.validators import list_of_dicts_validator

from ckanext.versioned_datastore.logic.validators import (
    not_missing,
    not_empty,
    resource_id_exists,
    ignore_missing,
    boolean_validator,
    is_queryable_resource_id,
    int_validator,
)


def vds_data_add() -> dict:
    return {
        "resource_id": [not_empty, str, resource_id_exists],
        "replace": [not_missing, boolean_validator],
        # TODO: can we make this work?
        "records": [ignore_missing, list_of_dicts_validator],
    }


def vds_data_delete() -> dict:
    return {
        "resource_id": [not_empty, str, resource_id_exists],
    }


def vds_data_sync() -> dict:
    return {
        "resource_id": [not_empty, str, resource_id_exists],
        "full": [ignore_missing, boolean_validator],
    }


def vds_data_get() -> dict:
    return {
        "resource_id": [not_empty, str, is_queryable_resource_id],
        "record_id": [not_empty, str],
        "version": [ignore_missing, int_validator],
    }
