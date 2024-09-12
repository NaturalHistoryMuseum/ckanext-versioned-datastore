from ckanext.versioned_datastore.logic.multi.schema import multi_params
from ckanext.versioned_datastore.logic.validators import (
    not_missing,
    not_empty,
    ignore_missing,
    boolean_validator,
    url_safe,
)


def vds_slug_create() -> dict:
    return {
        **multi_params,
        "pretty_slug": [ignore_missing, boolean_validator],
        "nav_slug": [ignore_missing, boolean_validator],
    }


def vds_slug_resolve() -> dict:
    return {
        "slug": [str],
    }


def vds_slug_edit() -> dict:
    return {
        "current_slug": [not_missing, not_empty, str],
        "new_reserved_slug": [not_missing, not_empty, str, url_safe],
    }
