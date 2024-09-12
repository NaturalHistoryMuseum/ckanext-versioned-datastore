import bisect
from typing import Optional

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
    return get_schema_versions()


@action(schema.vds_version_record(), helptext.vds_version_record, get=True)
def vds_version_record(resource_id: str, record_id: str):
    query = DirectQuery(
        [resource_id],
        None,
        Q("term", **{DocumentField.ID: record_id}),
    )
    request = SearchRequest(query, force_no_version=True)
    response = request.run()
    return sorted(hit.version for hit in response.hits)


@action(schema.vds_version_resource(), helptext.vds_version_resource, get=True)
def vds_version_resource(resource_id: str):
    database = get_database(resource_id)
    versions = {}

    for version in database.get_versions():
        versions[version] = {
            "total": database.search(version).count(),
            # todo: changes and field_count
        }

    return versions


@action(schema.vds_version_round(), helptext.vds_version_round, get=True)
def vds_version_round(resource_id: str, version: Optional[int] = None):
    database = get_database(resource_id)
    versions = database.get_versions()

    if not versions:
        # something isn't right, just set to None
        return None
    elif version is None or version >= versions[-1]:
        # cap the requested version to the latest version
        return versions[-1]
    elif version < versions[0]:
        # use the requested version if it's lower than the lowest available version
        return version
    else:
        # find the lowest, nearest version to the requested one
        position = bisect.bisect_right(versions, version)
        return versions[position - 1]
