import logging

from splitgill.ingestion.converters import RecordToMongoConverter
from splitgill.ingestion.feeders import IngestionFeeder, BaseRecord
from splitgill.ingestion.ingesters import Ingester
from splitgill.mongo import get_mongo

from .. import stats
from ... import common

log = logging.getLogger(__name__)


class DeletionRecord(BaseRecord):
    """
    A record that represents a deleted record to the converter object.
    """

    def __init__(self, version, resource_id, record_id):
        '''
        :param version: the version of this operation
        :param resource_id: the resource id of the resource we're deleting from
        :param record_id: the record to be deleted
        '''
        super(DeletionRecord, self).__init__(version)
        self.resource_id = resource_id
        self.record_id = record_id

    def convert(self):
        """
        Converts the data into the form to be stored in mongo under the "data" field. As
        this is a deletion record the return is an empty dict to signify the deletion.

        :return: an empty dict
        """
        # to signal a deletion we're using an empty dict
        return {}

    @property
    def id(self):
        """
        Returns the id of the record to delete.

        :return: the record id
        """
        return self.record_id

    @property
    def mongo_collection(self):
        """
        Returns the name of the collection in mongo that this record exists in (if
        indeed it does).

        :return: the resource id which is used as the collection name too
        """
        return self.resource_id


class DeletionFeeder(IngestionFeeder):
    """
    A feeder for deletion records.
    """

    def __init__(self, version, resource_id):
        '''
        :param version: the version of data to be fed
        :param resource_id: the resource id for which the data applies
        '''
        super(DeletionFeeder, self).__init__(version)
        self.resource_id = resource_id

    @property
    def source(self):
        """
        This is used for stats/logging and as it the name of the source of that data, in
        this case we just return "deletion" always because deletions are a special case.

        :return: "deletion", always
        """
        return 'deletion'

    def records(self):
        """
        Generator function which yields DeletionRecord objects.

        :return: yields DeletionRecords
        """
        with get_mongo(common.CONFIG, collection=self.resource_id) as mongo:
            # loop through records in mongo
            for record in mongo.find(projection=['id', 'data']):
                # only delete the record if it hasn't already been deleted
                if record['data']:
                    yield DeletionRecord(self.version, self.resource_id, record['id'])


def delete_resource_data(resource_id, version, start):
    """
    Update all the records in mongo to a deleted state (empty data). Essentially we
    present an empty dict as the new data version for all records in the collection that
    don't already have this state.

    :param resource_id: the resource id
    :param version: the version to put this ingest in as
    :param start: the start time of the deletion
    :return: True if the deletion was successful, False if not
    """
    log.info(f'Starting deletion for {resource_id}')

    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(resource_id, stats.INGEST, version, start)
    # create a feeder object to feed the existing records into the converter
    feeder = DeletionFeeder(version, resource_id)
    # create a converter object to actually create the updates that are run on the mongo collection
    converter = RecordToMongoConverter(version, start)
    # create an ingester using our deletion feeder and the converter
    ingester = Ingester(version, feeder, converter, common.CONFIG)
    # setup monitoring on the ingester so that we can update the database with stats about the
    # ingestion as it progresses
    stats.monitor_ingestion(stats_id, ingester)

    try:
        ingester.ingest()
        return True
    except Exception as e:
        stats.mark_error(stats_id, e)
        log.exception(f'An error occurred during data deletion of {resource_id}')
        return False


class ReplaceDeletionFeeder(IngestionFeeder):
    """
    A feeder for deletion records that come from a replacement upload.
    """

    def __init__(self, version, resource_id, tracker, original_source):
        '''
        :param version: the version of data to be fed
        :param resource_id: the resource id for which the data applies
        :param tracker: the InclusionTracker object
        :param original_source: the name of the original resource data source
        '''
        super(ReplaceDeletionFeeder, self).__init__(version)
        self.resource_id = resource_id
        self.tracker = tracker
        self.original_source = original_source

    @property
    def source(self):
        return self.original_source

    def records(self):
        """
        Generator function which yields DeletionRecord objects. When the replace flag is
        true during ingestion this indicates that any records not present in the new
        resource data should be deleted. By using the tracker this feeder can yield
        record objects which represent a record that was not included in a new version
        of a resource's data for deletion.

        :return: yields DeletionRecords
        """
        with get_mongo(common.CONFIG, collection=self.resource_id) as mongo:
            # this finds all the records that haven't been updated in the given version
            for mongo_doc in mongo.find({'latest_version': {'$lt': self.version}}):
                if not self.tracker.was_included(mongo_doc['id']):
                    # delete
                    yield DeletionRecord(
                        self.version, self.resource_id, mongo_doc['id']
                    )
