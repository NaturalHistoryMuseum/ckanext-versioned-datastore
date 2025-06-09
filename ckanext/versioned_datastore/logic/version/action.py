import bisect
from typing import List, Optional

from ckantools.decorators import action
from elasticsearch_dsl import Q
from splitgill.indexing.fields import DocumentField

from ckanext.versioned_datastore.lib.query.schema import get_schema_versions
from ckanext.versioned_datastore.lib.query.search.query import DirectQuery
from ckanext.versioned_datastore.lib.query.search.request import SearchRequest
from ckanext.versioned_datastore.lib.utils import get_database
from ckanext.versioned_datastore.logic.version import helptext, schema


@action(schema.vds_version_schema(), helptext.vds_version_schema, get=True)
def vds_version_schema():
    """
    Retrieves all the query schema versions that are available.

    :returns: a list of query schema versions.
    """
    return get_schema_versions()


@action(schema.vds_version_record(), helptext.vds_version_record, get=True)
def vds_version_record(resource_id: str, record_id: str) -> List[int]:
    """
    Retrieves all the versions available for a record in a resource. These will be
    returned as a list of ints in ascending order.

    :param resource_id: the resource ID
    :param record_id: the record ID
    :returns: a list of versions, or an empty list if the record doesn't exist
    """
    query = DirectQuery(
        [resource_id],
        None,
        Q('term', **{DocumentField.ID: record_id}),
    )
    request = SearchRequest(query, force_no_version=True)
    response = request.run()
    return sorted(hit.version for hit in response.hits)


@action(schema.vds_version_resource(), helptext.vds_version_resource, get=True)
def vds_version_resource(resource_id: str) -> List[int]:
    """
    Returns a list of the available versions for a datastore resource. These will be in
    ascending order.

    :param resource_id: the resource ID
    :returns: the versions in ascending order
    """
    database = get_database(resource_id)
    return database.get_versions()


@action(schema.vds_version_round(), helptext.vds_version_round, get=True)
def vds_version_round(resource_id: str, version: Optional[int] = None):
    """
    Given a resource ID and a version, rounds the version to the lowest, nearest version
    of the resource (i.e. rounds it down). If no version parameter is provided, or the
    provided version parameter higher than the current version of the resource, then the
    latest version of the resource is returned.

    :param resource_id: the resource ID
    :param version: the version to round (default is None)
    :returns: the rounded version
    """
    database = get_database(resource_id)
    versions = database.get_versions()

    if version is None or version >= versions[-1]:
        # cap the requested version to the latest version
        return versions[-1]
    elif version < versions[0]:
        # use the requested version if it's lower than the lowest available version
        return version
    else:
        # find the lowest, nearest version to the requested one
        position = bisect.bisect_right(versions, version)
        return versions[position - 1]
