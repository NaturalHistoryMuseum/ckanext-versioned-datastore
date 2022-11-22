import logging
from datetime import datetime

from .indexing import ResourceIndexRequest, index_resource
from .ingestion import deletion
from .ingestion.ingesting import ingest_resource
from .utils import check_version_is_valid
from .. import common
from ..datastore_utils import get_latest_version

log = logging.getLogger(__name__)


class ResourceImportRequest(object):
    """
    Class representing a request to import new data into a resource.

    We use a class like this for two reasons, firstly to avoid having a long list of
    arguments passed through to queued functions, and secondly because rq by default
    logs the arguments sent to a function and if the records argument is a large list of
    dicts this becomes insane.
    """

    def __init__(self, resource, version, replace, records=None, api_key=None):
        '''
        :param resource: the resource we're going to import (this must be the resource dict)
        :param version: the version of the resource to import
        :param replace: whether to replace the existing data or not
        :param records: a list of dicts to import, or None if the data is coming from URL or file
        '''
        self.resource = resource
        self.version = version
        self.replace = replace
        self.records = records
        self.api_key = api_key
        self.resource_id = self.resource['id']

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.records is not None:
            records = len(self.records)
        else:
            records = 0
        return (
            f'Import on {self.resource_id}, version {self.version}, replace: {self.replace}, '
            f'records: {records}'
        )


def import_resource_data(request):
    """
    Ingests the resource data into mongo and then, if needed, indexes it into
    elasticsearch. If the data argument is None (note, not falsey or an empty list,
    actually None) then the resource's url field is used as the source of the data.

    This function is blocking so it should be called through the background task queue to avoid
    blocking up a CKAN thread.

    :param request: the ResourceImportRequest object describing the resource import we need to do
    """
    # first, double check that the version is valid
    if not check_version_is_valid(request.resource_id, request.version):
        # log and silently skip this import
        log.info(
            f'Skipped importing data for {request.resource_id} at version {request.version} '
            f'as the version is invalid'
        )
        return

    log.info(
        f'Starting data import for {request.resource_id} at version {request.version}'
    )

    # ingest the resource into mongo
    did_ingest = ingest_resource(
        request.version,
        common.CONFIG,
        request.resource,
        request.records,
        request.replace,
        request.api_key,
    )
    if did_ingest:
        # find out what the latest version in the index is
        latest_index_version = get_latest_version(request.resource_id)

        # index the resource from mongo into elasticsearch. This will only index the records that
        # have changed between the latest index version and the newly ingested version
        index_resource(
            ResourceIndexRequest(
                request.resource, latest_index_version, request.version
            )
        )

        log.info(
            f'Ingest and index complete for {request.resource_id} at version '
            f'{request.version}'
        )


class ResourceDeletionRequest:
    """
    Class representing a request to delete all of a resource's data.

    We use a class like this for two reasons, firstly to avoid having a long list of
    arguments passed through to queued functions, and secondly because rq by default
    logs the arguments sent to a function and if the records argument is a large list of
    dicts this becomes insane.
    """

    def __init__(self, resource, version):
        '''
        :param resource: the resource we're going to delete (this must be the resource dict)
        :param version: the version of the resource to delete
        '''
        self.resource = resource
        self.version = version
        self.resource_id = resource['id']

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'Deletion of {self.resource_id} at version {self.version}'


def delete_resource_data(request):
    """
    Deletes all the resource's data. This involves ingesting a new version where all
    fields in each record are missing and then indexing this new version.

    This function is blocking so it should be called through the background task queue to avoid
    blocking up a CKAN thread.

    :param request: the ResourceDeletionRequest object describing the resource deletion we need to
                    do
    """
    # first, double check that the version is valid
    if not check_version_is_valid(request.resource_id, request.version):
        # log and silently skip this import
        log.info(
            f'Skipped importing data for {request.resource_id} at version {request.version} '
            f'as the version is invalid'
        )
        return

    log.info(
        f'Starting data deletion for {request.resource_id} at version {request.version}'
    )

    # store a start time, this will be used as the ingestion time of the records
    start = datetime.now()
    # delete the data in mongo
    did_delete = deletion.delete_resource_data(
        request.resource_id, request.version, start
    )
    if did_delete:
        # find out what the latest version in the index is
        latest_index_version = get_latest_version(request.resource_id)

        # index the resource from mongo into elasticsearch. This will only index the records that
        # have changed between the latest index version and the newly ingested version
        index_resource(
            ResourceIndexRequest(
                request.resource, latest_index_version, request.version
            )
        )

        log.info(
            f'Deletion complete for {request.resource_id} at version {request.version}'
        )
