from ..validators import (
    not_empty,
    ignore_missing,
    int_validator,
    is_queryable_resource_id,
)


def vds_version_schema() -> dict:
    return {}


def vds_version_record() -> dict:
    return {
        "resource_id": [not_empty, str, is_queryable_resource_id],
        "record_id": [not_empty, str],
    }


def vds_version_resource() -> dict:
    return {
        "resource_id": [not_empty, str, is_queryable_resource_id],
    }


def vds_version_round() -> dict:
    return {
        "resource_id": [not_empty, str, is_queryable_resource_id],
        "version": [ignore_missing, int_validator],
    }
