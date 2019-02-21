from eevee.ingestion.feeders import BaseRecord


class DatastoreRecord(BaseRecord):
    '''
    Represents a record from a feeder which needs to be ingested into mongo.
    '''

    def __init__(self, version, record_id, data, resource_id):
        '''
        :param version: the version of this record
        :param record_id: the record's id
        :param data: a dict containing the fields and values for the record
        :param resource_id: the resource id this record belongs to
        '''
        super(DatastoreRecord, self).__init__(version)
        self.record_id = record_id
        self.data = data
        self.resource_id = resource_id

    def convert(self):
        '''
        Converts the record into a suitable format for storage in mongo. For us this just means we
        return the data dict.

        :return: a dict ready for storage in mongo
        '''
        return self.data

    @property
    def id(self):
        '''
        Returns the id of the record.

        :return: the id
        '''
        return self.record_id

    @property
    def mongo_collection(self):
        '''
        Returns the name of the collection in mongo which should store this record's data.

        :return: the name of the target mongo collection
        '''
        return self.resource_id
