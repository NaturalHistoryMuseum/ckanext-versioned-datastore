from ckanext.datastore.logic.schema import json_validator
from ckanext.versioned_datastore.logic.validators import ignore_missing, not_missing


def vds_schema_latest() -> dict:
    return {}


def vds_schema_validate() -> dict:
    return {
        'query': [not_missing, json_validator],
        'query_version': [ignore_missing, str],
    }
