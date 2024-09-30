import re
from typing import Optional

from math import isfinite

from ckan import logic
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.utils import is_datastore_resource

boolean_validator = toolkit.get_validator("boolean_validator")
ignore_missing = toolkit.get_validator("ignore_missing")
int_validator = toolkit.get_validator("int_validator")
not_missing = toolkit.get_validator("not_missing")
not_empty = toolkit.get_validator("not_empty")
resource_id_exists = toolkit.get_validator("resource_id_exists")
email_validator = toolkit.get_validator("email_validator")
one_of = toolkit.get_validator("one_of")


def float_validator(value, context) -> float:
    """
    Checks if the value can be parsed as a float and returns it if it can, otherwise
    raises Invalid. Rejects NaN and inf.

    :param value: the value
    :param context: the context
    :return: a float
    """
    try:
        value = float(value)
        if isfinite(value):
            return value
    except (ValueError, TypeError):
        pass
    raise toolkit.Invalid("Invalid float value")


def url_safe(value, context):
    """
    Checks if the value is safe to be included in a URL as a slug.

    :param value: the value to check
    :param context: the context in which to check
    """
    if not re.match("^[A-Za-z0-9-_]+$", value):
        raise toolkit.Invalid(
            "Only a-z, 0-9, hyphens (-) and underscores (_) are valid characters"
        )
    else:
        return value


def check_resource_id(resource_id: str, context: Optional[dict]) -> bool:
    # TODO: does this context include auth for the calling user?
    context = context.copy() if context is not None else {}

    try:
        # check access (this also checks if the resource exists)
        toolkit.check_access("resource_show", context, {"id": resource_id})
    except logic.NotFound:
        return False

    # check is in datastore
    return is_datastore_resource(resource_id)


def is_queryable_resource_id(value: str, context: Optional[dict]):
    if not check_resource_id(value, context):
        raise toolkit.Invalid(f"Resource {value} is not a datastore resource")
    return value


def are_queryable_resource_ids(value: str, context: Optional[dict]):
    if context is None:
        context = {}

    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, list):
        raise toolkit.Invalid("Invalid list of resource ID strings")

    return [
        resource_id for resource_id in value if check_resource_id(resource_id, context)
    ]