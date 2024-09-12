from ckantools.decorators import action

from ckanext.versioned_datastore.lib.utils import is_datastore_resource
from ckanext.versioned_datastore.logic.resource import helptext, schema


@action(schema.vds_resource_check(), helptext.vds_resource_check)
def vds_resource_check(resource_id: str) -> bool:
    return is_datastore_resource(resource_id)
