import itertools
import logging
import numbers

import abc
import openpyxl
import requests
import six
import xlrd
from backports import csv
from eevee.ingestion.converters import RecordToMongoConverter
from eevee.ingestion.feeders import IngestionFeeder, BaseRecord
from eevee.ingestion.ingesters import Ingester
from eevee.mongo import get_mongo
from openpyxl.cell.read_only import EmptyCell

from ckanext.versioned_datastore.lib import stats
from ckanext.versioned_datastore.lib.utils import download_to_temp_file, CSV_FORMATS, TSV_FORMATS, \
    XLS_FORMATS, XLSX_FORMATS


log = logging.getLogger(__name__)


class DatastoreRecordConverter(RecordToMongoConverter):

    def __init__(self, version, ingestion_time):
        super(DatastoreRecordConverter, self).__init__(version, ingestion_time)

    def diff_data(self, existing_data, new_data):
        # even if the data hasn't changed we want to store a new version (the diff will just be
        # empty) as it means we can treat missing records from a new resource version as ones that
        # shouldn't be indexed by checking which records were updated in each version
        return True, super(DatastoreRecordConverter, self).diff_data(existing_data, new_data)[1]


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
        # where the source data dicts don't have ids and the user just wants to replace the existing
        # data
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
        for number, data in enumerate(self.data):
            yield self.create_record(number, data)


class SVFeeder(URLDatastoreFeeder):

    def __init__(self, version, resource_id, id_offset, url, dialect, default_encoding=u'utf-8'):
        super(SVFeeder, self).__init__(version, resource_id, id_offset, url)
        self.dialect = dialect
        self.default_encoding = default_encoding

    def line_iterator(self, r):
        for line in r.iter_lines(decode_unicode=True):
            if isinstance(line, unicode):
                yield line
            else:
                yield unicode(line, self.default_encoding)

    def records(self):
        # stream the csv file from the url
        with requests.get(self.url, stream=True) as response:
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
            for number, row in enumerate(rows):
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
            for number, row in enumerate(rows):
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

    with get_mongo(config, collection=resource_id) as mongo:
        doc_with_max_id = mongo.find_one(sort=[(u'id', -1)])
        id_offset = 0 if not doc_with_max_id else doc_with_max_id[u'id']

    if data is not None:
        # if there is data provided, use the API feeder
        return APIDatastoreFeeder(version, resource_id, id_offset, data)
    else:
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


def ingest_resource(version, start, config, resource, data):
    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(resource[u'id'], stats.INGEST, version, start)
    # work out which feeder to use for the resource
    feeder = get_feeder(config, version, resource, data)
    # if the return is None then no feeder can be matched and the data is uningestible :(
    if feeder is None:
        return False
    # register a monitor to keep the stats (and therefore the user) up to date
    feeder.register_monitor(stats.ingestion_monitor(stats_id))

    # create our custom datastore converter object
    converter = DatastoreRecordConverter(version, start)
    # create an ingester using our datastore feeder and the datastore converter
    ingester = Ingester(version, feeder, converter, config)
    try:
        ingest_stats = ingester.ingest()
        stats.finish_operation(stats_id, ingest_stats)
        return True
    except Exception as e:
        stats.mark_error(stats_id, e.message)
        log.exception(u'An error occurred during ingestion of {}'.format(resource[u'id']))
        return False
