import logging
import time
from datetime import datetime

from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib import utils
from ckanext.versioned_datastore.lib.indexing.indexing import index_resource
from ckanext.versioned_datastore.lib.ingestion import deletion
from ckanext.versioned_datastore.lib.ingestion.ingesting import ingest_resource
from ckanext.versioned_datastore.lib.indexing.indexing import ResourceIndexRequest
from ckanext.versioned_datastore.lib.stats import get_last_ingest

log = logging.getLogger(__name__)


def check_version_is_valid(resource_id, version):
    '''
    Checks that the given version is valid for the given resource id. Note that we check the ingest
    version not the indexed version as this is the source of truth about the versions of the
    resource we know about.

    The version must be greater than the latest ingested version or there must not be any ingested
    versions available.

    :param resource_id: the resource's id
    :param version: the version to check
    '''
    # retrieve the latest ingested version
    ingest_version = get_last_ingest(resource_id)
    # if there is a current version of the resource data the proposed version must be newer
    return ingest_version is None or version > ingest_version.version


def get_resource(resource_id, attempts=10, backoff=1):
    '''
    Given a resource id, returns its resource dict. This function will attempt to get the resource
    a number of times with a backoff between each attempt. This is silly but useful as when queueing
    a job right after creating a resource, the resource may not have become available yet in Solr.

    :param resource_id: the resource id
    :param attempts: the number of attempts to try and get the resource dict
    :param backoff: the time in seconds to wait between each attempt
    :return: the resource dict
    '''
    while True:
        try:
            # retrieve the resource
            return toolkit.get_action(u'resource_show')({u'ignore_auth': True}, {u'id': resource_id})
        except toolkit.ObjectNotFound:
            attempts -= 1
            if attempts < 0:
                raise
            time.sleep(backoff)


class ResourceImportRequest(object):
    '''
    Class representing a request to import new data into a resource. We use a class like this for
    two reasons, firstly to avoid having a long list of arguments passed through to queued
    functions, and secondly because rq by default logs the arguments sent to a function and if the
    records argument is a large list of dicts this becomes insane.
    '''

    def __init__(self, resource_id, version, replace, records=None, api_key=None):
        '''
        :param resource_id: the id of the resource to import
        :param version: the version of the resource to import
        :param replace: whether to replace the existing data or not
        :param records: a list of dicts to import, or None if the data is coming from URL or file
        '''
        self.resource_id = resource_id
        self.version = version
        self.replace = replace
        self.records = records
        self.api_key = api_key

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.records is not None:
            records = len(self.records)
        else:
            records = 0
        return u'Import on {}, version {}, replace: {}, records: {}'.format(self.resource_id,
                                                                            self.version,
                                                                            self.replace, records)


def import_resource_data(request):
    '''
    Ingests the resource data into mongo and then, if needed, indexes it into elasticsearch. If the
    data argument is None (note, not falsey or an empty list, actually None) then the resource's url
    field is used as the source of the data.

    This function is blocking so it should be called through the background task queue to avoid
    blocking up a CKAN thread.

    :param request: the ResourceImportRequest object describing the resource import we need to do
    '''
    # first, double check that the version is valid
    if not check_version_is_valid(request.resource_id, request.version):
        # log and silently skip this import
        log.info(u'Skipped importing data for {} at version {} as the version is invalid'.format(
            request.resource_id, request.version))
        return

    log.info(u'Starting data import for {} at version {}'.format(request.resource_id,
                                                                 request.version))

    # then, retrieve the resource dict
    resource = get_resource(request.resource_id)
    # store a start time, this will be used as the ingestion time of the records
    start = datetime.now()
    # ingest the resource into mongo
    did_ingest = ingest_resource(request.version, start, utils.CONFIG, resource, request.records,
                                 request.replace, request.api_key)
    if did_ingest:
        # find out what the latest version in the index is
        latest_index_version = utils.get_latest_version(request.resource_id)

        # index the resource from mongo into elasticsearch. This will only index the records that
        # have changed between the latest index version and the newly ingested version
        index_resource(ResourceIndexRequest(resource, latest_index_version, request.version))

        log.info(u'Ingest and index complete for {} at version {}'.format(request.resource_id,
                                                                          request.version))


class ResourceDeletionRequest(object):
    '''
    Class representing a request to delete all of a resource's data. We use a class like this for
    two reasons, firstly to avoid having a long list of arguments passed through to queued
    functions, and secondly because rq by default logs the arguments sent to a function and if the
    records argument is a large list of dicts this becomes insane.
    '''

    def __init__(self, resource_id, version):
        '''
        :param resource_id: the id of the resource to import
        :param version: the version of the resource to import
        '''
        self.resource_id = resource_id
        self.version = version

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return u'Deletion of {} at version {}'.format(self.resource_id, self.version)


def delete_resource_data(request):
    '''
    Deletes all the resource's data. This involves ingesting a new version where all fields in each
    record are missing and then indexing this new version.

    This function is blocking so it should be called through the background task queue to avoid
    blocking up a CKAN thread.

    :param request: the ResourceDeletionRequest object describing the resource deletion we need to
                    do
    '''
    # first, double check that the version is valid
    if not check_version_is_valid(request.resource_id, request.version):
        # log and silently skip this import
        log.info(u'Skipped importing data for {} at version {} as the version is invalid'.format(
            request.resource_id, request.version))
        return

    log.info(u'Starting data deletion for {} at version {}'.format(request.resource_id,
                                                                   request.version))

    # then, retrieve the resource dict
    resource = get_resource(request.resource_id)
    # store a start time, this will be used as the ingestion time of the records
    start = datetime.now()
    # delete the data in mongo
    did_delete = deletion.delete_resource_data(request.resource_id, request.version, start)
    if did_delete:
        # find out what the latest version in the index is
        latest_index_version = utils.get_latest_version(request.resource_id)

        # index the resource from mongo into elasticsearch. This will only index the records that
        # have changed between the latest index version and the newly ingested version
        index_resource(ResourceIndexRequest(resource, latest_index_version, request.version))

        log.info(u'Deletion complete for {} at version {}'.format(request.resource_id,
                                                                  request.version))
