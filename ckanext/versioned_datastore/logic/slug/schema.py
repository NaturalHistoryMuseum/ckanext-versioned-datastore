from ckanext.versioned_datastore.logic.multi.schema import multi_params
from ckanext.versioned_datastore.logic.validators import (
    boolean_validator,
    ignore_missing,
    not_empty,
    not_missing,
    url_safe,
)


def vds_slug_create() -> dict:
    return {
        **multi_params,
        'pretty_slug': [ignore_missing, boolean_validator],
        'nav_slug': [ignore_missing, boolean_validator],
    }


def vds_slug_resolve() -> dict:
    return {
        'slug': [str],
    }


def vds_slug_reserve() -> dict:
    return {
        'current_slug': [not_missing, not_empty, str],
        'new_reserved_slug': [not_missing, not_empty, str, url_safe],
    }
