class IngestionException(Exception):
    '''
    Represents an exception that has occurred during ingestion.
    '''
    pass


class UnsupportedDataSource(IngestionException):
    '''
    Should be raised when the data source we are attempting to ingest isn't one we can ingest - i.e.
    the format isn't one we support.
    '''

    def __init__(self, url):
        '''
        :param url: the url that couldn't be ingested
        '''
        super(UnsupportedDataSource, self).__init__(u'Could not find reader for {}'.format(url))
        self.url = url


class InvalidId(IngestionException):
    '''
    Should be raised when the data source we are attempting to ingest contains at least one row with
    an _id field which has a non-integer value.
    '''

    def __init__(self, row_number, row, cause=None):
        '''
        :param row_number: the row number (1-indexed, excluding the header)
        :param row: the row (this should be a dict
        :param cause: optional cause exception, for example a ValueError thrown by int(row[u'_id')
        '''
        message = u'Row {} had an invalid integer id: "{}"'.format(row_number, row[u'_id'])
        if cause is not None:
            message = u'{} [{}: {}]'.format(message, cause.__class__.__name__, unicode(cause))

        super(InvalidId, self).__init__(message)
        self.row_number = row_number
        self.row = row
        self.cause = cause


class DuplicateDataSource(IngestionException):
    '''
    Should be raised when the data source we are attempting to ingest is the same as the last
    successful ingest's data source.
    '''

    def __init__(self, file_hash):
        '''
        :param file_hash: the file hash that clashed
        '''
        super(DuplicateDataSource, self).__init__(
            u'This file has been ingested before, ignoring [hash: {}]'.format(file_hash))
        self.file_hash = file_hash
