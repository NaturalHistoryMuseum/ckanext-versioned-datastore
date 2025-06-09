import json
from collections import OrderedDict
from typing import Dict, List, Optional

from ckan import model

from ...model.details import DatastoreResourceDetails


def create_details(
    resource_id: str,
    version: int,
    columns: List[str],
    file_hash: Optional[str] = None,
) -> int:
    """
    Helper for creating a DatastoreResourceDetails object.

    :param resource_id: the resource id of the ingested resource
    :param version: the version being ingested
    :param columns: the columns present in the ingested resource
    :param file_hash: the computed file hash of the uploaded data
    :returns: the id of the created row
    """
    columns_str = json.dumps(columns)
    details = DatastoreResourceDetails(
        resource_id=resource_id,
        version=version,
        columns=columns_str,
        file_hash=file_hash,
    )
    details.add()
    details.commit()
    return details.id


def get_details(resource_id, version):
    """
    Retrieve the details for a resource at a given version.

    :param resource_id: the resource id
    :param version: the version to get the details at
    :returns: None or a DatastoreResourceDetails object
    """
    return (
        model.Session.query(DatastoreResourceDetails)
        .filter(DatastoreResourceDetails.resource_id == resource_id)
        .filter(DatastoreResourceDetails.version == version)
        .first()
    )


def get_all_details(
    resource_id, up_to_version=None
) -> Dict[int, DatastoreResourceDetails]:
    """
    Retrieve all the details for a resource and return them as an OrderedDict using the
    versions as the keys. If the up_to_version parameter is passed then any versions
    beyond it are ignored and not returned in the resulting OrderedDict.

    :param resource_id: the resource id
    :param up_to_version: the maximum version to include in the resulting OrderedDict
        (inclusive)
    :returns: None or an OrderedDict of version: DatastoreResourceDetails objects in
        ascending order
    """
    query = (
        model.Session.query(DatastoreResourceDetails)
        .filter(DatastoreResourceDetails.resource_id == resource_id)
        .order_by(DatastoreResourceDetails.version.asc())
    )

    all_details = OrderedDict()
    for details in query:
        if up_to_version is not None and details.version > up_to_version:
            break
        else:
            all_details[details.version] = details

    return all_details


def get_details_at(
    resource_id: str, version: Optional[int] = None
) -> Optional[DatastoreResourceDetails]:
    """
    Retrieve the details that apply at the given version. If the version is None (the
    default) then the latest details are returned.

    :param resource_id: the resource ID to get the details for
    :param version: the version of the details to get (or None for latest)
    :returns: None if no details were found, or a DatastoreResourceDetails object
    """
    all_details = query = (
        model.Session.query(DatastoreResourceDetails)
        .filter(DatastoreResourceDetails.resource_id == resource_id)
        .order_by(DatastoreResourceDetails.version.asc())
    ).all()

    if not all_details:
        return None

    if version is None:
        return all_details[-1]

    candidate = None
    for details in query:
        if version >= details.version:
            candidate = details
        else:
            break
    return candidate


def get_last_file_hash(resource_id):
    """
    Retrieves the most recent file hash and returns it. If there is no most recent file
    hash (either because there isn't a file hash present or because there aren't any
    details rows available) then this function returns None.

    :param resource_id: the resource id
    :returns: None or the most recent file hash
    """
    last_details = (
        model.Session.query(DatastoreResourceDetails)
        .filter(DatastoreResourceDetails.resource_id == resource_id)
        .order_by(DatastoreResourceDetails.version.desc())
        .first()
    )

    return last_details.file_hash if last_details is not None else None
