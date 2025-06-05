from typing import Optional

from ckan.logic import ValidationError
from ckan.plugins import toolkit
from ckantools.decorators import action
from jsonschema.exceptions import ValidationError

from ckanext.versioned_datastore.lib.query.schema import (
    get_latest_query_version,
    validate_query,
)
from ckanext.versioned_datastore.logic.schema import helptext, schema


@action(schema.vds_schema_latest(), helptext.vds_schema_latest, get=True)
def vds_schema_latest():
    """
    Retrieves the latest query schema version and returns it.

    :returns: the query schema version
    """
    return get_latest_query_version()


@action(schema.vds_schema_validate(), helptext.vds_schema_validate, get=True)
def vds_schema_validate(query: dict, query_version: Optional[str] = None):
    """
    Validates the given query against the given query schema version. If the query
    version is not provided, the latest query schema version is used.

    :param query: the query to validate
    :param query_version: the query schema version to validate against (default is None
        which means use the latest query schema version)
    :returns: True if the schema is valid, otherwise raises a CKAN ValidationError
    """
    if query_version is None:
        query_version = get_latest_query_version()
    try:
        validate_query(query, query_version)
    except ValidationError as e:
        raise toolkit.ValidationError(e.message)
    return True
