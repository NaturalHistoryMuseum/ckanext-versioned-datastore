from .stats import get_last_ingest


def check_version_is_valid(resource_id, version):
    """
    Checks that the given version is valid for the given resource id. Note that we check
    the ingest version not the indexed version as this is the source of truth about the
    versions of the resource we know about.

    The version must be greater than the latest ingested version or there must not be any ingested
    versions available.

    :param resource_id: the resource's id
    :param version: the version to check
    """
    # retrieve the latest ingested version
    ingest_version = get_last_ingest(resource_id)
    # if there is a current version of the resource data the proposed version must be newer
    return ingest_version is None or version > ingest_version.version
