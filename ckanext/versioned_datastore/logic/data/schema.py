from ckanext.versioned_datastore.logic.validators import (
    boolean_validator,
    ignore_missing,
    int_validator,
    not_empty,
    not_missing,
    resource_id_exists,
    validate_datastore_resource_id,
)


def vds_data_add() -> dict:
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'replace': [not_missing, boolean_validator],
    }


def vds_data_delete() -> dict:
    return {
        'resource_id': [not_empty, str, resource_id_exists],
    }


def vds_data_sync() -> dict:
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'full': [ignore_missing, boolean_validator],
    }


def vds_data_get() -> dict:
    return {
        'resource_id': [not_empty, str, validate_datastore_resource_id],
        'record_id': [not_empty, str],
        'version': [ignore_missing, int_validator],
    }
