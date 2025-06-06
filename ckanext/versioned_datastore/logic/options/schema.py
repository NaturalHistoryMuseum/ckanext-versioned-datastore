from ckanext.datastore.logic.schema import json_validator
from ckanext.versioned_datastore.logic.validators import (
    ignore_missing,
    int_validator,
    not_empty,
    not_missing,
    resource_id_exists,
)


def vds_options_get() -> dict:
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'version': [ignore_missing, int_validator],
    }


def vds_options_update() -> dict:
    return {
        'resource_id': [not_empty, str, resource_id_exists],
        'overrides': [not_missing, json_validator],
    }
