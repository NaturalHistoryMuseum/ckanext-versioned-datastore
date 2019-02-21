import contextlib
import itertools
import logging
import numbers
from contextlib import closing

import abc
import openpyxl
import requests
import six
import xlrd
from backports import csv
from eevee import diffing
from eevee.ingestion.converters import RecordToMongoConverter
from eevee.ingestion.feeders import IngestionFeeder, BaseRecord
from eevee.ingestion.ingesters import Ingester
from eevee.mongo import get_mongo, MongoOpBuffer
from eevee.utils import chunk_iterator
from openpyxl.cell.read_only import EmptyCell
from pymongo import UpdateOne

from ckanext.versioned_datastore.lib import stats
from ckanext.versioned_datastore.lib.utils import download_to_temp_file, CSV_FORMATS, TSV_FORMATS, \
    XLS_FORMATS, XLSX_FORMATS, is_datastore_only_resource

log = logging.getLogger(__name__)


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


@six.add_metaclass(abc.ABCMeta)
class DatastoreFeeder(IngestionFeeder):
    '''
    Base abstract class for the datastore feeders.
    '''

    def __init__(self, version, resource_id, id_offset):
        '''
        :param version: the version of the data we're going to read
        :param resource_id: the resource id
        '''
        super(DatastoreFeeder, self).__init__(version)
        self.resource_id = resource_id
        self.id_offset = id_offset

    def create_record(self, number, data):
        '''
        Creates a record given the row number of the record (1-based) and the data from that row as
        a dict.

        :param number: the row number (1-based of the record)
        :param data: the row's data as a dictionary
        :return: a new DatastoreRecord object
        '''
        # if the record has an _id column then we use it, if it doesn't then we just use the index
        # of the record in the source plus the offset value. This accommodates the simple scenario
        # where the source data dicts don't have ids and the user just wants to add to the existing
        record_id = data.pop(u'_id', self.id_offset + number)
        return DatastoreRecord(self.version, record_id, data, self.resource_id)


@six.add_metaclass(abc.ABCMeta)
class URLDatastoreFeeder(DatastoreFeeder):

    def __init__(self, version, resource_id, id_offset, url):
        '''
        :param url: the url where the data resides
        '''
        super(URLDatastoreFeeder, self).__init__(version, resource_id, id_offset)
        self.url = url

    @property
    def source(self):
        '''
        Where the data we've read came from. We just return the url.

        :return: the url from which the data is collected and returned in the form of records
        '''
        return self.url


class APIDatastoreFeeder(DatastoreFeeder):

    def __init__(self, version, resource_id, id_offset, data):
        super(APIDatastoreFeeder, self).__init__(version, resource_id, id_offset)
        self.data = data

    @property
    def source(self):
        return u'API'

    def records(self):
        for number, data in enumerate(self.data, start=1):
            yield self.create_record(number, data)


class SVFeeder(URLDatastoreFeeder):

    def __init__(self, version, resource_id, id_offset, url, dialect, default_encoding=u'utf-8'):
        super(SVFeeder, self).__init__(version, resource_id, id_offset, url)
        self.dialect = dialect
        self.default_encoding = default_encoding

    def line_iterator(self, response):
        '''
        Iterate over the lines in the response, decoding each into UTF-8.

        :param response: a requests response object
        :return: generator object that produces lines of text
        '''
        for line in response.iter_lines():
            yield line.decode(self.default_encoding)

    def records(self):
        # stream the file from the url (note that we have to use closing here because the ability to
        # directly use with on requests.get wasn't added until 2.18.0 and we're on 2.10.0 :(
        with closing(requests.get(self.url, stream=True)) as response:
            reader = csv.DictReader(self.line_iterator(response), dialect=self.dialect)
            for number, data in enumerate(reader, start=1):
                # yield a new record for each row
                yield self.create_record(number, data)


class CSVFeeder(SVFeeder):
    '''
    Feeds records from a CSV.
    '''

    def __init__(self, version, resource_id, id_offset, url):
        super(CSVFeeder, self).__init__(version, resource_id, id_offset, url, u'excel')


class TSVFeeder(SVFeeder):
    '''
    Feeds records from a TSV.
    '''

    def __init__(self, version, resource_id, id_offset, url):
        super(TSVFeeder, self).__init__(version, resource_id, id_offset, url, u'excel-tab')


class XLSFeeder(URLDatastoreFeeder):
    '''
    Feeds records from an XLS (old excel) file.
    '''

    def records(self):
        # download the url into a temporary file and then read from that. This is necessary as xls
        # files can't be streamed, they have to be completed loaded into memory
        with download_to_temp_file(self.url) as temp:
            # open the xls file up
            book = xlrd.open_workbook(temp.name)
            # select the first sheet by default
            sheet = book.sheet_by_index(0)
            # get a row generator
            rows = sheet.get_rows()
            # assume the first row is the header
            header = [unicode(cell.value) for cell in next(rows)]
            # then read all the other rows as data
            for number, row in enumerate(rows, start=1):
                data = {}
                for field, cell in zip(header, row):
                    # if the cell is the id column, it contains a number and the number is an
                    # integer, convert it from a float to an int
                    if (field == u'_id' and cell.ctype == xlrd.XL_CELL_NUMBER and
                            cell.value.is_integer()):
                        data[field] = int(cell.value)
                    elif cell == xlrd.XL_CELL_EMPTY:
                        # ignore empty cells
                        continue
                    else:
                        # otherwise just use the value
                        data[field] = unicode(cell.value)
                # yield a new record
                yield self.create_record(number, data)


class XLSXFeeder(URLDatastoreFeeder):
    '''
    Feeds records from an XLSX (new excel) file.
    '''

    def records(self):
        # download the url into a temporary file and then read from that. This is necessary as xlsx
        # files can't be streamed, they have to be completed loaded into memory
        with download_to_temp_file(self.url) as temp:
            wb = openpyxl.load_workbook(temp, read_only=True)
            # get a generator for the rows in the active workbook
            rows = wb.active.rows
            # always treat the first row as a header
            header = [unicode(cell.value) for cell in next(rows)]
            # then read all the other rows as data
            for number, row in enumerate(rows, start=1):
                data = {}
                for field, cell in zip(header, row):
                    # if the cell is the id column and it contains a number make sure it stays a
                    # number
                    if field == u'_id' and isinstance(cell.value, numbers.Number):
                        data[field] = cell.value
                    # ignore empty cells
                    elif isinstance(cell, EmptyCell):
                        continue
                    else:
                        # convert everything else to unicode
                        data[field] = unicode(cell.value)
                # yield a new record
                yield self.create_record(number, data)


FEEDER_FORMAT_MAP = dict(
    itertools.chain(
        zip(CSV_FORMATS, itertools.repeat(CSVFeeder)),
        zip(TSV_FORMATS, itertools.repeat(TSVFeeder)),
        zip(XLS_FORMATS, itertools.repeat(XLSFeeder)),
        zip(XLSX_FORMATS, itertools.repeat(XLSXFeeder)),
    )
)


def get_feeder(config, version, resource, data=None):
    '''
    Returns the correct feeder object for the given resource. The feeder object is created based on
    the format property on the resource, not on the URL. If no feeder can be matched to the resource
    then None is returned.

    :param config: the config object
    :param version: the version of the resource
    :param resource: the resource dict
    :param data: the data passed in the request (None if not passed)
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
    elif not is_datastore_only_resource(resource[u'url']):
        # otherwise we need to use the URL on the resource check to see if the format is set and
        # isn't empty/None (hence the use of get)
        if resource.get(u'format', False):
            # get the format and convert it to lowercase
            resource_format = resource[u'format'].lower()
            if resource_format in FEEDER_FORMAT_MAP:
                return FEEDER_FORMAT_MAP[resource_format](version, resource_id, id_offset,
                                                          resource[u'url'])

    # if nothing works out, return None to indicate that no feeder could be matched for the resource
    return None


class UnchangedRecordTracker:
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


def ingest_resource(version, start, config, resource, data, replace):
    # cache the resource id as we use it a few times
    resource_id = resource[u'id']

    # work out which feeder to use for the resource
    feeder = get_feeder(config, version, resource, data)
    # if the return is None then no feeder can be matched and the data is uningestible :(
    if feeder is None:
        return False

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

        return True
    except Exception as e:
        stats.mark_error(stats_id, e)
        log.exception(u'An error occurred during ingestion of {}'.format(resource_id))
        return False
