from typing import Optional

from ckantools.decorators import action
from jsonschema.exceptions import ValidationError

from ckan.logic import ValidationError
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.query.schema import (
    validate_query,
    get_latest_query_version,
)
from ckanext.versioned_datastore.logic.schema import helptext, schema


@action(schema.vds_schema_latest(), helptext.vds_schema_latest, get=True)
def vds_schema_latest():
    return get_latest_query_version()


@action(schema.vds_schema_validate(), helptext.vds_schema_validate, get=True)
def vds_schema_validate(query: dict, query_version: Optional[str] = None):
    if query_version is None:
        query_version = get_latest_query_version()
    try:
        validate_query(query, query_version)
    except ValidationError as e:
        raise toolkit.ValidationError(e.message)
    return True
