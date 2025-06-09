from ckantools.validators import object_validator

from ckanext.versioned_datastore.logic.download.arg_objects import (
    DerivativeArgs,
    NotifierArgs,
    QueryArgs,
    ServerArgs,
)
from ckanext.versioned_datastore.logic.validators import ignore_missing, not_missing


def vds_download_queue() -> dict:
    return {
        'query': [not_missing, object_validator(QueryArgs)],
        'file': [
            not_missing,
            object_validator(DerivativeArgs),
        ],
        # called 'file' instead of derivative to make its purpose clearer to the end user
        'server': [ignore_missing, object_validator(ServerArgs)],
        'notifier': [ignore_missing, object_validator(NotifierArgs)],
    }


def vds_download_regenerate():
    return {
        'download_id': [not_missing, str],
        'server': [ignore_missing, object_validator(ServerArgs)],
        'notifier': [ignore_missing, object_validator(NotifierArgs)],
    }
