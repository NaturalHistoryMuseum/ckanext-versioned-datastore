from ckan.plugins import toolkit
from jsonschema.exceptions import ValidationError

from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.query.search.request import SearchRequest
from ckanext.versioned_datastore.lib.utils import get_available_datastore_resources


def make_request(data_dict: dict) -> SearchRequest:
    """
    Given an action data dict, creates a SearchRequest object for it.

    :param data_dict: the action data dict
    :returns: a SearchRequest object
    """
    # if no resource IDs are provided, default to all resources available to the user
    resource_ids = data_dict.get('resource_ids', [])
    if not resource_ids:
        resource_ids = list(get_available_datastore_resources())

    query = SchemaQuery(
        resource_ids,
        data_dict.get('version'),
        data_dict.get('query'),
        data_dict.get('query_version'),
    )

    # check the query is valid before we go any further
    try:
        query.validate()
    except ValidationError as e:
        raise toolkit.ValidationError(e.message)

    request = SearchRequest(
        query,
        size=data_dict.get('size'),
        after=data_dict.get('after'),
        data_dict=data_dict,
    )

    # ignore any resources that are unavailable for whatever reason
    request.add_param('ignore_unavailable', True)

    return request
