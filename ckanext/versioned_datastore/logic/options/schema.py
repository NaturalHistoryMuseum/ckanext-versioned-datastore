from ckanext.datastore.logic.schema import json_validator
from ckanext.versioned_datastore.logic.validators import (
    not_empty,
    resource_id_exists,
    ignore_missing,
    int_validator,
    not_missing,
)


def vds_options_get() -> dict:
    return {
        "resource_id": [not_empty, str, resource_id_exists],
        "version": [ignore_missing, int_validator],
    }


def vds_options_update() -> dict:
    return {
        "resource_id": [not_empty, str, resource_id_exists],
        "overrides": [not_missing, json_validator],
    }
