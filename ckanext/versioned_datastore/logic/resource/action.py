from ckantools.decorators import action

from ckanext.versioned_datastore.lib.utils import is_datastore_resource
from ckanext.versioned_datastore.logic.resource import helptext, schema


@action(schema.vds_resource_check(), helptext.vds_resource_check)
def vds_resource_check(resource_id: str) -> bool:
    """
    Checks if the given resource ID is a datastore resource. This is defined as whether
    it has data in Elasticsearch or not.

    :param resource_id: the resource ID
    :returns: True if the resource has data in Elasticsearch, False if not
    """
    return is_datastore_resource(resource_id)
