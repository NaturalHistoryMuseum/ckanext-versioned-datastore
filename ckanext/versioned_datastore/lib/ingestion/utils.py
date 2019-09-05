import gzip
import hashlib
import sqlite3
import tempfile
from contextlib import contextmanager, closing

import requests


def compute_hash(fp):
    '''
    Given a file pointer like object, computes the sha1 hash of its data. The file pointer is reset
    after use.

    :param fp: a file pointer like object
    :return: the sha1 hexdigest
    '''
    hasher = hashlib.sha1()
    with ensure_reset(fp):
        while True:
            chunk = fp.read(16384)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


@contextmanager
def download_to_temp_file(url, headers=None, compress=True, chunk_size=1024):
    '''
    Streams the data from the given URL and saves it in a temporary file. The (named) temporary file
    is then yielded to the caller for use. Once the context collapses the temporary file is removed.

    If the compress parameter is passed as True (the default) the data will be downloaded and
    written to a file using GZIP and a GzipFile pointer will be returned.

    :param url: the url to stream the data from
    :param headers: a dict of headers to pass with the request
    :param compress: whether to compress the downloaded data when storing it, if so a GzipFile
                     pointer will be returned (default: True)
    :param chunk_size: the number of bytes to read at a time from the url stream
    '''
    headers = headers if headers else {}
    # open up the url for streaming
    with closing(requests.get(url, stream=True, headers=headers)) as r:
        # check that we got a response we can use!
        r.raise_for_status()

        def download(fp):
            # iterate over the data from the url stream in chunks
            for chunk in r.iter_content(chunk_size=chunk_size):
                # only write chunks with data in them
                if chunk:
                    # write the chunk to the file
                    fp.write(chunk)

        # create a temporary file to store the data in
        with tempfile.NamedTemporaryFile(delete=True) as temp:
            if compress:
                with gzip.open(temp.name, mode=u'wb') as f:
                    download(f)
                with gzip.open(temp.name, mode=u'rb') as g:
                    yield g
            else:
                download(temp)
                temp.seek(0)
                yield temp


@contextmanager
def ensure_reset(file_pointer):
    '''
    Context manager which resets (seeks to 0) the passed file pointer after use.

    :param file_pointer: the file pointer to reset
    '''
    try:
        yield
    finally:
        file_pointer.seek(0)


class InclusionTracker(object):
    '''
    Class that tracks the ids of records that have been included in an ingestion event but we're
    actually modified. The ids are stored in a temporary sqlite database in case there are too many
    to store in memory. The objects created from this class definition should be used as context
    managers like so:

    with InclusionTracker(ingester) as tracker:
        ...
    '''

    def __init__(self, ingester):
        '''
        :param ingester: the ingester object - we'll use the update signal to track the ids
        '''
        self.ingester = ingester
        self.temporary_file = None
        self.tracker_db = None

    def __enter__(self):
        self.temporary_file = tempfile.NamedTemporaryFile(delete=True)
        self.tracker_db = sqlite3.connect(self.temporary_file.name)
        self.tracker_db.execute(u'create table ids (id integer primary key)')

        @self.ingester.update_signal.connect_via(self.ingester)
        def on_update(_sender, record, doc):
            # this implies that the record update wasn't written to mongo because the record
            # hasn't changed in this new version
            if not doc:
                self.tracker_db.execute(u'insert into ids(id) values (?)', (record.id,))

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tracker_db.commit()
        self.tracker_db.close()
        self.temporary_file.close()

    def was_included(self, record_id):
        '''
        Checks whether the given record id was included in the ingestion but not updated as the data
        was the same.

        :param record_id: the record id
        :return: True if the record id was included but not updated and False if not
        '''
        with self.tracker_db:
            cursor = self.tracker_db.cursor()
            cursor.execute(u'select 1 from ids where id = ?', (record_id,))
            return cursor.fetchone() is not None


def iter_universal_lines(fp, read_size=65536):
    '''
    Given a file object, read data from it, convert various newline types to \n and then yield
    lines as strings (i.e. bytes in python2). This is a way round the problem of not being able to
    reopen a file already opened in rb mode in rU mode. The line endings recognised by this function
    are "\n", "\r" and "\r\n".

    :param fp: the file object to read from, this must be open in rb mode
    :param read_size: the number of bytes to read at a time from the file object (default: 65536)
    :return: a generator which yields lines as strings
    '''
    cache = b''
    while True:
        chunk = fp.read(read_size)
        if chunk:
            # switch the \r\n and \r line endings for \n endings
            chunk = chunk.replace(b'\r\n', b'\n').replace(b'\r', b'\n')
            for character in chunk:
                cache += character
                if character == b'\n':
                    # if the cache isn't just a new line, yield it
                    if cache != b'\n':
                        yield cache
                    # reset the cache for the next line
                    cache = b''
        else:
            # if there's any data left in the cache, yield it
            if cache:
                yield cache
            break
