import logging
import time
from datetime import datetime

import dictdiffer
from eevee.mongo import get_mongo, MongoOpBuffer
from eevee.utils import serialise_diff
from pymongo import UpdateOne

from ckan import logic
from ckan.logic import NotFound
from ckanext.versioned_datastore.lib.indexing import index_resource
from ckanext.versioned_datastore.lib.ingesting import ingest_resource
from ckanext.versioned_datastore.lib import stats


log = logging.getLogger(__name__)


def index_action_remove(config, resource_id, version, ingestion_time):
    '''
    Performs the index action "remove". This involves "removing" all data not updated in the given
    version by creating a new version of each record with empty data as the new data. This means
    that when the new version is indexed by our DatastoreIndexer the records with no data are
    treated as deleted because we have a no-data-skipping-filter in place.

    :param config: the config object
    :param resource_id: the resource id of the resource
    :param version: all records in this resource that don't have this version as their
                    latest_version will be "removed"
    :param ingestion_time: the time to mark as the "last_ingested" time of the records
    '''
    with MongoOpBuffer(config, get_mongo(config, collection=resource_id)) as mongo_buffer:
        # find and iterate through all the records that weren't updated in the given version
        for mongo_doc in mongo_buffer.mongo.find({u'latest_version': {u'$ne': version}}):
            # create a diff between current data in the record and an empty dict
            diff = list(dictdiffer.diff(mongo_doc[u'data'], {}))
            # organise our update op
            update = {
                u'$set': {
                    u'data': {},
                    u'latest_version': version,
                    u'last_ingested': ingestion_time,
                    u'diffs.{}'.format(version): serialise_diff(diff),
                },
                u'$addToSet': {u'versions': version}
            }
            # add the op to the buffer so that the update operations are handled in bulk
            mongo_buffer.add(UpdateOne({u'id': mongo_doc[u'id']}, update))


def get_resource(resource_id, attempts=3, backoff=1):
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


def import_resource_data(resource_id, config, version, index_action, data):
    '''
    Ingests the resource data into mongo and then, if needed, indexes it into elasticsearch. If the
    data argument is None (note, not falsey or an empty list, actually None) then the resource's url
    field is used as the source of the data.

    This function is blocking so it should be called through the background task queue to avoid
    blocking up a CKAN thread.

    :param resource_id: the resource id
    :param config: the eevee config object
    :param version: the data version
    :param index_action: directs the import code to either skip indexing all together, or perform a
                         specific action before indexing. The options are:
                            - skip: skips indexing altogether, this therefore allows the updating a
                                    resource's data across multiple requests. If this argument is
                                    used then the only way the newly ingested version will become
                                    visible in the index is if a final request is made with one of
                                    the other index_actions below.
                            - remove: before the data in the new version is indexed, the records
                                      that were not included in the version are flagged as deleted
                                      meaning they will not be indexed in the new version. Note that
                                      even if a record's data hasn't changed in the new version
                                      (i.e. the v1 record looks the same as the v2 record) it will
                                      not be deleted. This index action is intended to fulfill the
                                      requirements of a typical user uploading a csv to the site -
                                      they expect the indexed resource to contain the data in the
                                      uploaded csv and nothing else.
                            - retain: just index the records, regardless of whether they were
                                      updated in the last version or not. This action allows for
                                      indexing partial updates to the resources' data.
    :param data: a list of dicts to import, or None if the url of the resource should be used
                 instead
    '''
    # first, retrieve the resource dict
    resource = get_resource(resource_id)
    # store a start time, this will be used as the ingestion time of the records
    start = datetime.now()
    # ingest the resource into mongo
    did_ingest = ingest_resource(version, start, config, resource, data)
    if did_ingest is not None and index_action != u'skip':
        # if the index action is remove we need to do that before indexing the resource's data
        if index_action == u'remove':
            index_action_remove(config, resource_id, version, start)

        # index the resource from mongo into elasticsearch
        index_resource(version, config, resource)
