import logging

from eevee.ingestion.converters import RecordToMongoConverter
from eevee.ingestion.feeders import IngestionFeeder, BaseRecord
from eevee.ingestion.ingesters import Ingester
from eevee.mongo import get_mongo

from ckanext.versioned_datastore.lib import utils, stats

log = logging.getLogger(__name__)


class DeletionRecord(BaseRecord):
    '''
    A record that represents a deleted record to the converter object.
    '''

    def __init__(self, version, resource_id, record_id):
        super(DeletionRecord, self).__init__(version)
        self.resource_id = resource_id
        self.record_id = record_id

    def convert(self):
        # to signal a deletion we're using an empty dict
        return {}

    @property
    def id(self):
        return self.record_id

    @property
    def mongo_collection(self):
        return self.resource_id


class DeletionFeeder(IngestionFeeder):
    '''
    A feeder for deletion records.
    '''

    def __init__(self, version, resource_id):
        super(DeletionFeeder, self).__init__(version)
        self.resource_id = resource_id

    @property
    def source(self):
        return u'deletion'

    def records(self):
        with get_mongo(utils.CONFIG, collection=self.resource_id) as mongo:
            # loop through records in mongo
            for record in mongo.find(projection=[u'id', u'data']):
                # only delete the record if it hasn't already been deleted
                if record[u'data']:
                    yield DeletionRecord(self.version, self.resource_id, record[u'id'])


def delete_resource_data(resource_id, version, start):
    '''
    Update all the records in mongo to a deleted state (empty data). Essentially we present an empty
    dict as the new data version for all records in the collection that don't already have this
    state.

    :param resource_id: the resource id
    :param version: the version to put this ingest in as
    :param start: the start time of the deletion
    :return: True if the deletion was successful, False if not
    '''
    log.info(u'Starting deletion for {}'.format(resource_id))

    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(resource_id, stats.INGEST, version, start)
    # create a feeder object to feed the existing records into the converter
    feeder = DeletionFeeder(version, resource_id)
    # create a converter object to actually create the updates that are run on the mongo collection
    converter = RecordToMongoConverter(version, start)
    # create an ingester using our deletion feeder and the converter
    ingester = Ingester(version, feeder, converter, utils.CONFIG)
    # setup monitoring on the ingester so that we can update the database with stats about the
    # ingestion as it progresses
    stats.monitor_ingestion(stats_id, ingester)

    try:
        ingester.ingest()
        return True
    except Exception as e:
        stats.mark_error(stats_id, e)
        log.exception(u'An error occurred during data deletion of {}'.format(resource_id))
        return False
