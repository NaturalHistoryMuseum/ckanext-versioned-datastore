from splitgill.ingestion.feeders import BaseRecord


def _convert(data):
    """
    Converts the given dict into a suitable format for storage in mongo. For us this
    means:

        - replacing '.' with '_' as neither mongo nor elasticsearch can handle dots in field
          names as they both use the dot notation for nested field access
        - ignoring fields with no name (we use a falsey check on the field name) to ensure we
          don't create fields that are the empty string

    This function is recursive.

    :return: a dict ready for storage in mongo
    """
    converted = {}
    for field, value in data.items():
        # elasticsearch doesn't allow empty fields, plus it's silly so ignore them
        if not field:
            continue
        # mongo doesn't allow dots in keys so replace them with underscores
        field = field.replace('.', '_')
        if isinstance(value, dict):
            converted[field] = _convert(value)
        else:
            converted[field] = value
    return converted


class DatastoreRecord(BaseRecord):
    """
    Represents a record from a feeder which needs to be ingested into mongo.
    """

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
        """
        Converts the record into a suitable format for storage in mongo. For us this
        means:

            - replacing '.' with '_' as neither mongo nor elasticsearch can handle dots in field
              names as they both use the dot notation for nested field access
            - ignoring fields with no name (we use a falsey check on the field name) to ensure we
              don't create fields that are the empty string

        :return: a dict ready for storage in mongo
        """
        return _convert(self.data)

    @property
    def id(self):
        """
        Returns the id of the record.

        :return: the id
        """
        return self.record_id

    @property
    def mongo_collection(self):
        """
        Returns the name of the collection in mongo which should store this record's
        data.

        :return: the name of the target mongo collection
        """
        return self.resource_id
