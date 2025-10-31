import re
from math import isfinite
from typing import Iterable, Optional, Union

from ckan.model import Resource, Session
from ckan.plugins import toolkit

from ckanext.versioned_datastore.lib.utils import is_datastore_resource

boolean_validator = toolkit.get_validator('boolean_validator')
ignore_missing = toolkit.get_validator('ignore_missing')
int_validator = toolkit.get_validator('int_validator')
not_missing = toolkit.get_validator('not_missing')
not_empty = toolkit.get_validator('not_empty')
resource_id_exists = toolkit.get_validator('resource_id_exists')
email_validator = toolkit.get_validator('email_validator')
one_of = toolkit.get_validator('one_of')


def float_validator(value, context) -> float:
    """
    Checks if the value can be parsed as a float and returns it if it can, otherwise
    raises Invalid. Rejects NaN and inf.

    :param value: the value
    :param context: the context
    :returns: a float
    """
    try:
        value = float(value)
        if isfinite(value):
            return value
    except (ValueError, TypeError):
        pass
    raise toolkit.Invalid('Invalid float value')


def url_safe(value, context):
    """
    Checks if the value is safe to be included in a URL as a slug.

    :param value: the value to check
    :param context: the context in which to check
    """
    if not re.match('^[A-Za-z0-9-_]+$', value):
        raise toolkit.Invalid(
            'Only a-z, 0-9, hyphens (-) and underscores (_) are valid characters'
        )
    else:
        return value


def check_resource_id(resource_id: str, context: Optional[dict] = None) -> bool:
    context = context.copy() if context is not None else {}
    if context.get('user') is None:
        try:
            context['user'] = toolkit.g.get('user')
        except RuntimeError:
            # during e.g. testing we don't have access to toolkit.g
            pass

    # check it exists
    if not Session.query(Resource).get(resource_id):
        return False
    # check we have access to it
    try:
        toolkit.check_access('resource_show', context, {'id': resource_id})
        return True
    except toolkit.NotAuthorized:
        return False


def check_datastore_resource_id(
    resource_id: str, context: Optional[dict] = None
) -> bool:
    return check_resource_id(resource_id, context) and is_datastore_resource(
        resource_id
    )


def _deduplicate(values: Iterable[str]) -> Iterable[str]:
    """
    Simple util function to remove duplicate entries in an iterable.

    :param values: the iterable to deduplicate
    :returns: the deduplicated values as another iterable
    """
    seen = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        yield value


def validate_resource_ids(value: Union[str, list], context: Optional[dict] = None):
    if isinstance(value, str):
        value = value.split(',') if value else []
    if not isinstance(value, list):
        raise toolkit.Invalid('Invalid list of resource ID strings')

    if context is None:
        context = {}
    valid_resource_ids = [
        resource_id
        for resource_id in _deduplicate(value)
        if check_resource_id(resource_id, context)
    ]
    if value and not valid_resource_ids:
        # the user passed some resources, but none of them were datastore resources
        raise toolkit.Invalid('No resource IDs are available')

    return valid_resource_ids


def validate_datastore_resource_ids(
    value: Union[str, list], context: Optional[dict] = None
):
    if isinstance(value, str):
        value = value.split(',') if value else []
    if not isinstance(value, list):
        raise toolkit.Invalid('Invalid list of resource ID strings')

    if context is None:
        context = {}
    valid_resource_ids = [
        resource_id
        for resource_id in _deduplicate(value)
        if check_datastore_resource_id(resource_id, context)
    ]
    if value and not valid_resource_ids:
        # the user passed some resources, but none of them were datastore resources
        raise toolkit.Invalid('No resource IDs are datastore resources')

    return valid_resource_ids


def validate_datastore_resource_id(value: str, context: Optional[dict] = None):
    return validate_datastore_resource_ids([value], context)[0]
