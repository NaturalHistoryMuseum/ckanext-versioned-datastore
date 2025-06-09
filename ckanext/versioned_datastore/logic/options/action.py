from typing import Optional

from ckantools.decorators import action

from ckanext.versioned_datastore.lib.importing.options import (
    get_options,
    update_options,
)
from ckanext.versioned_datastore.lib.utils import (
    ReadOnlyResourceException,
    is_resource_read_only,
)
from ckanext.versioned_datastore.logic.options import helptext, schema


@action(schema.vds_options_get(), helptext.vds_options_get, get=True)
def vds_options_get(resource_id: str, version: Optional[int] = None) -> Optional[dict]:
    """
    Retrieves the options in use by Splitgill for the given resource at the given
    optional version.

    :param resource_id: the resource ID
    :param version: the version (default is None which means get the latest options)
    :returns: the options as a dict, or None if there are no options available
    """
    options = get_options(resource_id, version)
    return None if options is None else options.to_doc()


@action(schema.vds_options_update(), helptext.vds_options_update)
def vds_options_update(
    resource_id: str,
    overrides: dict,
) -> Optional[int]:
    """
    Updates the options for the given resource with the new values in the overrides
    parameter. The existing options are retrieved and then the new options in the
    overrides dict are written over the top. This allows incremental updates to the
    existing options. If no options exist, the default options are used as a base.

    :param resource_id: the resource ID
    :param overrides: a dict of override options to apply
    :returns: the new options version, or None if nothing was changed
    """
    if is_resource_read_only(resource_id):
        raise ReadOnlyResourceException('This resource has been marked as read only')

    return update_options(resource_id, overrides)
