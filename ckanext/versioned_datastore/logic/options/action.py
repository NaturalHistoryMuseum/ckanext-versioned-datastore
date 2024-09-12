from typing import Optional

from ckantools.decorators import action

from ckanext.versioned_datastore.lib.importing.options import (
    update_options,
    get_options,
)
from ckanext.versioned_datastore.lib.utils import (
    is_resource_read_only,
    ReadOnlyResourceException,
)
from ckanext.versioned_datastore.logic.options import helptext, schema


@action(schema.vds_options_get(), helptext.vds_options_get, get=True)
def vds_options_get(resource_id: str, version: Optional[int] = None) -> Optional[dict]:
    options = get_options(resource_id, version)
    return None if options is None else options.to_doc()


@action(schema.vds_options_update(), helptext.vds_options_update)
def vds_options_update(
    resource_id: str,
    overrides: dict,
) -> Optional[int]:
    if is_resource_read_only(resource_id):
        raise ReadOnlyResourceException("This resource has been marked as read only")

    return update_options(resource_id, overrides)
