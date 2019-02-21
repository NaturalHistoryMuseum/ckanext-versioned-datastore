import logging
import time
from datetime import datetime

from ckan import logic
from ckan.logic import NotFound
from ckanext.versioned_datastore.lib import utils
from ckanext.versioned_datastore.lib.indexing.indexing import index_resource
from ckanext.versioned_datastore.lib.ingestion.ingesting import ingest_resource
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
            return logic.get_action(u'resource_show')({}, {u'id': resource_id})
        except NotFound:
            attempts -= 1
            if attempts < 0:
                raise
            time.sleep(backoff)


def import_resource_data(resource_id, config, version, replace, data):
    '''
    Ingests the resource data into mongo and then, if needed, indexes it into elasticsearch. If the
    data argument is None (note, not falsey or an empty list, actually None) then the resource's url
    field is used as the source of the data.

    This function is blocking so it should be called through the background task queue to avoid
    blocking up a CKAN thread.

    :param resource_id: the resource id
    :param config: the eevee config object
    :param version: the data version
    :param replace: whether to replace the data from previous versions or not
    :param data: a list of dicts to import, or None if the url of the resource should be used
                 instead
    '''
    # first, double check that the version is valid
    if not check_version_is_valid(resource_id, version):
        # log and silently skip this import
        log.info(u'Skipped importing data for {} at version {} as the version is invalid'.format(
            resource_id, version))
        return

    # then, retrieve the resource dict
    resource = get_resource(resource_id)
    # store a start time, this will be used as the ingestion time of the records
    start = datetime.now()
    # ingest the resource into mongo
    did_ingest = ingest_resource(version, start, config, resource, data, replace)
    if did_ingest:
        # find out what the latest version in the index is
        latest_index_versions = utils.SEARCHER.get_index_versions([resource_id], prefixed=False)
        latest_index_version = latest_index_versions.get(resource_id, None)

        # index the resource from mongo into elasticsearch. This will only index the records that
        # have changed between the latest index version and the newly ingested version
        index_resource(resource, config, latest_index_version, version)
