import contextlib
import itertools
import logging

from contextlib2 import suppress
from eevee import diffing
from eevee.ingestion.converters import RecordToMongoConverter
from eevee.ingestion.ingesters import Ingester
from eevee.mongo import get_mongo, MongoOpBuffer
from eevee.utils import chunk_iterator
from pymongo import UpdateOne

from ckanext.versioned_datastore.lib import stats
from ckanext.versioned_datastore.lib.details import create_details
from ckanext.versioned_datastore.lib.ingestion.feeders import XLSXFeeder, XLSFeeder, TSVFeeder, \
    CSVFeeder, APIDatastoreFeeder
from ckanext.versioned_datastore.lib.utils import CSV_FORMATS, TSV_FORMATS, \
    XLS_FORMATS, XLSX_FORMATS, is_datastore_only_resource


log = logging.getLogger(__name__)


# the available formats and feeders
FEEDER_FORMAT_MAP = dict(
    itertools.chain(
        zip(CSV_FORMATS, itertools.repeat(CSVFeeder)),
        zip(TSV_FORMATS, itertools.repeat(TSVFeeder)),
        zip(XLS_FORMATS, itertools.repeat(XLSFeeder)),
        zip(XLSX_FORMATS, itertools.repeat(XLSXFeeder)),
    )
)


def get_feeder(config, version, resource, data=None, api_key=None):
    '''
    Returns the correct feeder object for the given resource. The feeder object is created based on
    the format property on the resource, not on the URL - i.e. if the URL ends in .csv we don't
    match it to the CSVFeeder, we only match to the CSV feeder if the format is set on the resource.
    If no feeder can be matched to the resource then None is returned.

    :param config: the config object
    :param version: the version of the resource
    :param resource: the resource dict
    :param data: the data passed in the request (None if not passed)
    :param api_key: the api key of the user who is requesting the ingest
    :return: a feeder object or None
    '''
    # extract the resource id from the resource dict
    resource_id = resource[u'id']

    # figure out what the current max id is in the collection, this will then be used as the offset
    # for new records entered into the collection if the records don't have ids of their own
    with get_mongo(config, collection=resource_id) as mongo:
        doc_with_max_id = mongo.find_one(sort=[(u'id', -1)])
        id_offset = 0 if not doc_with_max_id else doc_with_max_id[u'id']

    if data is not None:
        # if there is data provided, use the API feeder
        return APIDatastoreFeeder(version, resource_id, id_offset, data)

    # we don't work on datastore only resources as these are side loaded to avoid this process
    if not is_datastore_only_resource(resource[u'url']):
        # otherwise we need to use the URL on the resource check to see if the format is set and
        # isn't empty/None (hence the use of get)
        if resource.get(u'format', False):
            # get the format and convert it to lowercase
            resource_format = resource[u'format'].lower()
            if resource_format in FEEDER_FORMAT_MAP:
                is_upload = (resource[u'url_type'] == u'upload')
                return FEEDER_FORMAT_MAP[resource_format](version, resource_id, id_offset,
                                                          resource[u'url'], api_key, is_upload)

    # if nothing works out, return None to indicate that no feeder could be matched for the resource
    return None


class UnchangedRecordTracker(object):
    '''
    Class to track records that are included in versions but not changed. This allows us to provide
    the functionality whereby a new version's data replaces the existing data, i.e. essentially
    deletes the previous data by setting its data to {}.
    '''

    def __init__(self, config, resource_id, version):
        '''
        :param config: the eevee config object
        :param resource_id: the resource id
        :param version: the version ingested
        '''
        self.config = config
        self.resource_id = resource_id
        self.version = version

        # we use a temporary collection for some of the processing, let's give it a name that won't
        # clash with any other proper collections
        self.temp_collection = u'__unchanged_{}_{}'.format(self.resource_id, self.version)

    @contextlib.contextmanager
    def get_tracker_buffer(self):
        '''
        Returns an op buffer for the temporary collection tracking unchanged records.

        :return: an op buffer
        '''
        mongo = get_mongo(self.config, collection=self.temp_collection)
        with MongoOpBuffer(self.config, mongo) as op_buffer:
            op_buffer.mongo.create_index(u'id')
            yield op_buffer

    def remove_missing_records(self, ingester_start_time):
        '''
        To be called after all the data in a version has been ingested, this function actually
        removes the records that weren't in the latest version.

        This means marking records already in the collection that haven't already been marked as
        deleted and weren't included in the latest version's data as deleted. We do this by pushing
        a new version to the records where the data is {}. This means that when the record is
        indexed it will be ignored and effectively deleted.

        :param ingester_start_time: the start time of the ingestion process
        '''
        with get_mongo(self.config, collection=self.resource_id) as resource_mongo:
            with get_mongo(self.config, collection=self.temp_collection) as temp_mongo:
                # this finds all the records that haven't been updated in the given version
                condition = {u'latest_version': {u'$lt': self.version}}
                # loop through the not-updated records in chunks
                for chunk in chunk_iterator(resource_mongo.find(condition)):
                    # extract the ids of the records
                    chunk_ids = {mongo_doc[u'id'] for mongo_doc in chunk}
                    # create a set of the record ids in this chunk that were in the version but
                    # unchanged
                    unchanged_ids = \
                        {doc[u'id'] for doc in temp_mongo.find({u'id': {u'$in': list(chunk_ids)}})}

                    # we'll collect up a batch of update operations to send to mongo at once for
                    # max efficiency
                    op_batch = []
                    # now loop through all the records in the chunk
                    for mongo_doc in chunk:
                        # if the record hasn't already been removed and isn't in the unchanged set
                        # then it was not present at all in the new version and needs removing
                        if mongo_doc[u'data'] and mongo_doc[u'id'] not in unchanged_ids:
                            # create a diff between current data in the record and an empty dict
                            diff = diffing.SHALLOW_DIFFER.diff(mongo_doc[u'data'], {})
                            # organise our update op
                            update = {
                                u'$set': {
                                    # set the data to empty
                                    u'data': {},
                                    # update the latest version
                                    u'latest_version': self.version,
                                    # include the ingester start time
                                    u'last_ingested': ingester_start_time,
                                    # create a new diff from the last data on the record to empty
                                    u'diffs.{}'.format(self.version):
                                        diffing.format_diff(diffing.SHALLOW_DIFFER, diff),
                                },
                                # add the version to the versions array
                                u'$addToSet': {u'versions': self.version}
                            }
                            # add the update to the batch of ops
                            op_batch.append(UpdateOne({u'id': mongo_doc[u'id']}, update))

                    # update the records
                    if op_batch:
                        resource_mongo.bulk_write(op_batch)

                # clean up
                temp_mongo.drop()


def ingest_resource(version, start, config, resource, data, replace, api_key):
    '''
    Ingest a new version of a resource's data.

    :param version: the new version
    :param start: the start time of the ingestion
    :param config: the eevee config object
    :param resource: the resource dict
    :param data: the data to ingest (can be None if not using the API)
    :param replace: boolean indicating whether to replace the existing data or not
    :param api_key: the API key if the resource's CKAN URL is to be used as the source and the
                    resource is private
    :return: True if the ingest was successful, False if not
    '''
    # cache the resource id as we use it a few times
    resource_id = resource[u'id']

    # work out which feeder to use for the resource
    feeder = get_feeder(config, version, resource, data, api_key)
    # if the return is None then no feeder can be matched and the data is uningestible :(
    if feeder is None:
        log.info(u'The data for resource {} is uningestible, skipping'.format(resource_id))
        return False

    log.info(u'Starting ingest for {}'.format(resource_id))

    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(resource_id, stats.INGEST, version, start)

    # create our custom datastore converter object
    converter = RecordToMongoConverter(version, start)
    # create an ingester using our datastore feeder and the datastore converter
    ingester = Ingester(version, feeder, converter, config)
    # setup monitoring on the ingester so that we can update the database with stats about the
    # ingestion as it progresses
    stats.monitor_ingestion(stats_id, ingester)

    try:
        if replace:
            # create the tracker object and get the tracker buffer
            tracker = UnchangedRecordTracker(config, resource_id, version)
            with tracker.get_tracker_buffer() as tracker_buffer:
                # connect to the update signal on the ingester
                @ingester.update_signal.connect_via(ingester)
                def on_update(_sender, record, doc):
                    if replace and not doc:
                        # we got an update for a record but no update doc so it was unchanged, we
                        # need to add it to the tracker collection
                        tracker_buffer.add(UpdateOne({u'id': record.id},
                                                     {u'$set': {u'id': record.id}}, upsert=True))

                ingester.ingest()
                tracker.remove_missing_records(ingester.start)
        else:
            ingester.ingest()

        # we really don't care about errors
        with suppress(Exception):
            # create a details row
            create_details(resource_id, version, feeder.columns)

        return True
    except Exception as e:
        stats.mark_error(stats_id, e)
        log.exception(u'An error occurred during ingestion of {}'.format(resource_id))
        return False
