from datetime import datetime

from ckan.plugins import toolkit
from splitgill.utils import to_timestamp

from .meta import help, schema
from ckantools.decorators import action
from ...lib import common
from ...lib.datastore_utils import prefix_resource
from ...lib.downloads.download import queue_download
from ...lib.query.schema import (
    get_latest_query_version,
    validate_query,
    translate_query,
    hash_query,
)
from ...lib.query.utils import get_available_datastore_resources
from ...model.downloads import DatastoreDownload


@action(schema.datastore_queue_download(), help.datastore_queue_download)
def datastore_queue_download(
    email_address,
    context,
    query=None,
    query_version=None,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
    separate_files=True,
    format='csv',
    format_args=None,
    ignore_empty_fields=True,
    transform=None,
    slug_or_doi=None,
):
    """
    Starts a download of the data found by the given query parameters. This download is
    created asynchronously using the rq background job queue and a link to the results
    is emailed to the given email address when complete.

    :param email_address: the email address to send the download link to
    :param context: the context dict from the action call
    :param query: the query dict. If None (default) then an empty query is used
    :param query_version: the version of the query schema the query is using. If None (default) then
                          the latest query schema version is used
    :param version: the version to search the data at. If None (default) the current time is used
    :param resource_ids: the list of resource to search. If None (default) then all the resources
                         the user has access to are queried. If a list of resources are passed then
                         any resources not accessible to the user will be removed before querying
    :param resource_ids_and_versions: a dict of resources and versions to search each of them at.
                                      This allows precise searching of each resource at a specific
                                      parameter. If None (default) then the resource_ids parameter
                                      is used together with the version parameter. If this parameter
                                      is provided though, it takes priority over the resource_ids
                                      and version parameters.
    :param separate_files: whether to split the results into a file per resource or just put all
                           results in one file. The default is True - split results into a file per
                           resource.
    :param format: the format to download the data in. The default is csv.
    :param format_args: additional arguments for specific formats, e.g. extension names for DwC-A.
    :param ignore_empty_fields: whether to ignore fields with no data in them in the result set
                                and not write them into the download file(s). Default: True.
    :param transform: data transformation configuration.
    :param slug_or_doi: use instead of query, query_version, resource_ids, and resource_ids_and_versions;
                        retrieves these parameters from a saved query via a slug or a doi
    :return: a dict containing info about the background job that is doing the downloading and the
             download id
    """

    if slug_or_doi:
        try:
            saved_query = toolkit.get_action('datastore_resolve_slug')(
                context, {'slug': slug_or_doi}
            )
            query = saved_query.get('query')
            query_version = saved_query.get('query_version')
            resource_ids = saved_query.get('resource_ids')
            resource_ids_and_versions = saved_query.get('resource_ids_and_versions')
        except toolkit.ValidationError:
            # if the slug doesn't resolve, continue as normal
            pass

    if resource_ids_and_versions is None:
        resource_ids_and_versions = {}
    else:
        # use the resource_ids_and_versions dict first over the resource_ids and version params
        resource_ids = list(resource_ids_and_versions.keys())

    # figure out which resources should be searched
    resource_ids = get_available_datastore_resources(context, resource_ids)
    if not resource_ids:
        raise toolkit.ValidationError(
            "The requested resources aren't accessible to this user"
        )

    rounded_resource_ids_and_versions = {}
    # see if a version was provided, we'll use this is a resource id we're searching doesn't have a
    # directly assigned version (i.e. it was absent from the requested_resource_ids_and_versions
    # dict, or that parameter wasn't provided)
    if version is None:
        version = to_timestamp(datetime.now())
    for resource_id in resource_ids:
        # try to get the target version from the passed resource_ids_and_versions dict, but if
        # it's not in there, default to the version variable
        target_version = resource_ids_and_versions.get(resource_id, version)
        index = prefix_resource(resource_id)
        # round the version down to ensure we search the exact version requested
        rounded_version = common.SEARCH_HELPER.get_rounded_versions(
            [index], target_version
        )[index]
        if rounded_version is not None:
            # resource ids without a rounded version are skipped
            rounded_resource_ids_and_versions[resource_id] = rounded_version

    # setup the query
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()
    validate_query(query, query_version)
    search = translate_query(query, query_version)
    query_hash = hash_query(query, query_version)

    format_args = format_args or {}
    transform = transform or {}

    options = {
        'separate_files': separate_files,
        'format': format,
        'format_args': format_args,
        'ignore_empty_fields': ignore_empty_fields,
        'transform': transform,
    }
    download = DatastoreDownload(
        query_hash=query_hash,
        query=query,
        query_version=query_version,
        resource_ids_and_versions=rounded_resource_ids_and_versions,
        state='queued',
        options=options,
    )
    download.save()

    job = queue_download(
        email_address,
        download.id,
        query_hash,
        query,
        query_version,
        search.to_dict(),
        rounded_resource_ids_and_versions,
        separate_files,
        format,
        format_args,
        ignore_empty_fields,
        transform,
    )

    return {
        'queued_at': job.enqueued_at.isoformat(),
        'job_id': job.id,
        'download_id': download.id,
    }
