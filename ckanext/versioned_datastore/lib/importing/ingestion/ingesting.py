import contextlib
import gzip
import logging
import shutil
import tempfile
import zipfile

import codecs
import itertools
import math
import os
import simplejson
import unicodedata
from contextlib import suppress
from datetime import datetime
from splitgill.ingestion.converters import RecordToMongoConverter
from splitgill.ingestion.feeders import IngestionFeeder
from splitgill.ingestion.ingesters import Ingester
from splitgill.mongo import get_mongo

from . import exceptions
from .deletion import ReplaceDeletionFeeder
from .readers import get_reader, APIReader
from .records import DatastoreRecord
from .utils import download_to_temp_file, compute_hash, InclusionTracker
from .. import stats
from ..details import create_details, get_last_file_hash
from ....model.stats import ImportStats

log = logging.getLogger(__name__)


def ingest_resource(version, config, resource, data, replace, api_key):
    """
    Ingest a new version of a resource's data.

    :param version: the new version
    :param config: the splitgill config object
    :param resource: the resource dict
    :param data: the data to ingest (can be None if not using the API)
    :param replace: boolean indicating whether to replace the existing data or not
    :param api_key: the API key if the resource's CKAN URL is to be used as the source and the
                    resource is private
    :return: True if the ingest was successful, False if not
    """
    # cache the resource id as we use it a few times
    resource_id = resource['id']

    log.info(f'Starting validation for {resource_id}')
    # create a stats entry so that preparation progress can be tracked
    prep_stats_id = stats.start_operation(resource_id, stats.PREP, version)
    try:
        data_file_name, data_file_metadata = prepare_resource(
            resource, version, prep_stats_id, data, api_key
        )
    except Exception as e:
        stats.mark_error(prep_stats_id, e)
        log.info(
            f'Prep failed for resource {resource_id} due to {e.__class__.__name__}: {str(e)}'
        )
        if isinstance(e, exceptions.IngestionException):
            # these exceptions are expected (validation problems for example)
            return False
        else:
            raise
    else:
        stats.finish_operation(prep_stats_id, data_file_metadata['record_count'])

    log.info(f'Starting ingest for {resource_id}')
    start = datetime.now()
    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(resource_id, stats.INGEST, version, start)
    try:
        feeder = DatastoreFeeder(config, resource_id, version, data_file_name)
        converter = RecordToMongoConverter(version, start)
        ingester = Ingester(version, feeder, converter, config)
        # setup monitoring on the ingester so that we can update the database with stats about the
        # ingestion as it progresses
        stats.monitor_ingestion(stats_id, ingester)

        with InclusionTracker(ingester) as tracker:
            ingester.ingest()

            if replace:
                replace_feeder = ReplaceDeletionFeeder(
                    version, resource_id, tracker, feeder.source
                )
                # note that we use the same converter to ensure the ingestion time is the same
                replace_ingester = Ingester(version, replace_feeder, converter, config)
                # TODO: should we merge the replace ingest stats with the main ingest stats?
                replace_ingester.ingest()
        # we really don't care about errors
        with suppress(Exception):
            # create a details row
            create_details(
                resource_id,
                version,
                data_file_metadata['fields'],
                data_file_metadata['file_hash'],
            )

        return True
    except Exception as e:
        stats.mark_error(stats_id, e)
        log.exception(f'An error occurred during ingestion of {resource_id}')
        return False
    finally:
        # make sure we clean up the intermediate data file
        if os.path.exists(data_file_name):
            os.remove(data_file_name)


def prepare_resource(
    resource, version, stats_id, data=None, api_key=None, update_every=1000
):
    """
    Downloads, validates and then produces an intermediate file containing the rows from
    the resource to be ingested. If the data parameter is provided then it is used as
    the resource data, if not then the resource url is used and the data is extracted
    from the resource url.

    The intermediate file is a JSONL file (newline-delimited JSON using UTF-8 -
    http://jsonlines.org/) which is compressed using GZIP. The first line of the file contains
    metadata about the file including (but not necessary limited to):
        - version
        - resource id
        - fields present (in the order they appear in the source)
        - maximum id present in the source (if any ids were included)
        - record count
        - file hash (will be None if the data parameter is used)

    :param resource: the resource dict
    :param version: the version of the data
    :param stats_id: the stats id for this prep run
    :param data: optional data, if provided must be a list of dicts
    :param api_key: the API key of a user who can read the data, if indeed the data needs an API
                    key to get it. This is needed when the URL is the CKAN resource download URL
                    of a private resource. Can be None to indicate no API key is required
    :param update_every: the frequency with which to update the ImportStats (every x rows written to
                         the intermediate file format). Setting this too low will cause the database
                         to be written to a lot which could cause performance issues
    :return: the name of the intermediate file and the metadata dict
    """
    name = os.path.join(tempfile.gettempdir(), f'{resource["id"]}_{version}.jsonl.gz')

    try:
        with get_fp_and_reader_for_resource_data(resource, data, api_key) as (
            fp,
            reader,
        ):
            # first of all compute a hash of the file so that we can test to see if it's different
            # from the last file we ingested
            last_file_hash = get_last_file_hash(resource['id'])
            if fp is not None:
                file_hash = compute_hash(fp)
            else:
                # rather than hash the data list just use None
                file_hash = None

            # only raise a duplication error if the hashes are the same and they are both non-None
            if file_hash == last_file_hash and file_hash is not None:
                raise exceptions.DuplicateDataSource(file_hash)

            # next, run through the rows in the data to figure out how many rows there are and what
            # the maximum id is (if there is one). If there are validation errors in the data then
            # they will come out here
            record_count = 0
            max_id = -float('inf')
            for row in reader.iter_rows(fp):
                record_count += 1
                if '_id' in row:
                    max_id = max(row['_id'], max_id)

            # this metadata dict will be written out as the first row of the intermediate file
            metadata = {
                'fields': reader.get_fields(fp),
                'file_hash': file_hash,
                'record_count': record_count,
                'resource_id': resource['id'],
                'source': resource['url'] if data is None else 'API',
                'version': version,
            }
            if not math.isinf(max_id):
                # add the max id present if we found one
                metadata['max_id'] = max_id
            # allow the reader to modify the metadata before we write it out
            reader.modify_metadata(metadata)

            # create the intermediate file
            with gzip.open(name, mode='wb') as gzip_file:
                # ensure the data is written out in utf-8
                writer = codecs.getwriter('utf-8')(gzip_file)
                # write the metadata out as a single line first
                writer.write(simplejson.dumps(metadata, ensure_ascii=False) + '\n')
                # then write the rows out as single lines
                for count, row in enumerate(reader.iter_rows(fp), start=1):
                    row_data = simplejson.dumps(row, ensure_ascii=False)
                    # check that the unicode produced doesn't contain any crap characters that we
                    # won't be able to read during ingestion
                    if any(
                        unicodedata.category(character)[0] == 'C'
                        for character in row_data
                    ):
                        raise exceptions.InvalidCharacterException(count, row)

                    writer.write(row_data + '\n')

                    if count % update_every == 0:
                        stats.update_stats(
                            stats_id,
                            {
                                ImportStats.in_progress: True,
                                ImportStats.count: count,
                            },
                        )

            return name, metadata
    except Exception:
        # make sure we clean up the file if there was an error
        if os.path.exists(name):
            os.remove(name)
        raise


@contextlib.contextmanager
def get_fp_and_reader_for_resource_data(resource, data=None, api_key=None):
    """
    Context manager which given a resource, yields a file pointer to the downloaded
    resource source and a ResourceReader instance for reading the data from the file
    pointer.

    If the data parameter is passed (should be a list of dicts) then no file pointer is yielded
    (None is yielded instead). A ResourceReader instance is yielded as normal.

    :param resource: the resource dict
    :param data: optional data, if provided must be a list of dicts
    :param api_key: the API key of a user who can read the data, if indeed the data needs an API
                    key to get it. This is needed when the URL is the CKAN resource download URL
                    of a private resource. Can be None to indicate no API key is required
    :return: yields a file pointer and a ResourceReader instance
    """
    handled = False
    if data is None:
        # there will be a url in the resource dict
        url = resource['url']
        # there may not be a format in the resource dict
        resource_format = resource.get('format', None)
        if resource_format is not None:
            resource_format = resource_format.lower()
        # headers for the download request, note that we only incldue the auth in the request if
        # the url is for an uploaded file, this prevents leaking the credentials
        headers = (
            {'Authorization': api_key}
            if (resource.get('url_type', None) == 'upload' and api_key)
            else {}
        )

        if resource_format != 'zip':
            reader = get_reader(resource_format)
            if reader is not None:
                # if we got a reader, download the resource data to a temporary file
                with download_to_temp_file(
                    url, compress=reader.compressible, headers=headers
                ) as temp:
                    yield temp, reader
                    handled = True
        else:
            # the resource data source is a zip so we need to download the zip, look in it to see if
            # we can find a file to use and then use it. Apologies for the massively nested withs.
            with download_to_temp_file(
                url, compress=False, headers=headers
            ) as temp_download:
                with zipfile.ZipFile(temp_download) as temp_zip:
                    # sort the list of files so that we maintain a consistency when reading zips
                    for name in sorted(temp_zip.namelist()):
                        # use the file extension of the file to see if there's a reader we can use
                        reader = get_reader(os.path.splitext(name)[1][1:])
                        if reader is not None:
                            with temp_zip.open(name, 'r') as zipped_file:
                                with tempfile.NamedTemporaryFile(
                                    mode='w+b', delete=True
                                ) as temp:
                                    # extract the file data from the zip
                                    if reader.compressible:
                                        with gzip.open(temp.name, mode='wb') as g:
                                            shutil.copyfileobj(zipped_file, g)
                                        with gzip.open(temp.name, mode='rb') as g:
                                            yield g, reader
                                    else:
                                        shutil.copyfileobj(zipped_file, temp)
                                        temp.seek(0)
                                        yield temp, reader
                                    handled = True
                                    break
    else:
        yield None, APIReader(data)
        handled = True

    if not handled:
        raise exceptions.UnsupportedDataSource(resource.get('format', None))


class DatastoreFeeder(IngestionFeeder):
    """
    Ingestion feeder class for the versioned datastore.

    This feeder handles reading rows from the intermediate data file format produced by
    the prepare_resource function above.
    """

    def __init__(self, config, resource_id, version, data_file_name):
        '''
        :param config: the splitgill config object
        :param resource_id: the resource id
        :param version: the version of the data we're ingesting
        :param data_file_name: the name of the intermediate data file to read the data from
        '''
        super(DatastoreFeeder, self).__init__(version)
        self.config = config
        self.resource_id = resource_id
        self.data_file_name = data_file_name
        self._header = None

    def iter_rows(self, skip_header_row):
        """
        Returns a generator which can be used to iterate over the rows in the data file.
        This generator yields dicts and therefore handles reading and deserialising the
        rows so that you don't have to.

        :param skip_header_row: whether to skip the header row or yield it as if it was a normal row
        :return: a generator of dicts
        """
        with gzip.open(self.data_file_name, 'rb') as gzip_file:
            # the data file is always in utf-8 format
            reader = codecs.getreader('utf-8')(gzip_file)
            if skip_header_row:
                # skip the first line
                next(reader)
            for line in reader:
                yield simplejson.loads(line)

    def get_existing_max_id(self):
        """
        Figure out what the current max id is in this resource's collection.

        :return: the highest id in the collection currently (it'll be an int), or 0 if there
                aren't any documents in the collection
        """
        with get_mongo(self.config, collection=self.resource_id) as mongo:
            # sort by id descending to get the highest
            doc_with_max_id = mongo.find_one(sort=[('id', -1)])
            # find_one returns None if there aren't any matching documents
            if doc_with_max_id is None:
                return 0
            else:
                return doc_with_max_id['id']

    @property
    def header(self):
        """
        Property providing access to the header in the data file. This function caches
        the header dict in self._header for repeated use and thus avoids reading the
        file every time.

        :return: the header dict
        """
        if self._header is None:
            # load the header by just iterating over the first row in the data file
            self._header = next(self.iter_rows(skip_header_row=False))
        return self._header

    @property
    def source(self):
        """
        Returns the source of the ingestion. This comes from the data file.

        :return: the source name
        """
        return self.header['source']

    def records(self):
        """
        Returns a generator of DatastoreRecords using the rows in the data file.

        The id of each yielded record is determined using either the _id field in the row or by an
        incrementing counter. The counter starts at the maximum id we're aware of - either the
        highest id in the existing mongo collection (if there is one) or the highest id from the _id
        fields in the rows (if there is such a field). Using the highest value of these two sources
        allows us to handle data files where the case where the _id field is only included in some
        rows in the data file. For example, with this data:

            _id, field1, ...
            1,   some data
            2,   some other data
            ,    this one has no id
            3,   but this one does

        if we didn't account for the highest id in the data file the row without an _id (row 3)
        would be assigned _id = 3, but this would then be used by the next row which specifically
        says "I'm row 3" and thus clashing ids!

        :return: a generator of DatastoreRecords
        """
        # calculate the highest id we know about
        highest_id = max(self.get_existing_max_id(), self.header.get('max_id', 0))
        # create a generator which starts at the next number after the highest id
        new_id_generator = itertools.count(start=highest_id + 1, step=1)

        for data in self.iter_rows(skip_header_row=True):
            # see if there's an id defined by the row
            record_id = data.pop('_id', None)
            if record_id is None:
                # create a new id for this record
                record_id = next(new_id_generator)

            yield DatastoreRecord(self.version, record_id, data, self.resource_id)
