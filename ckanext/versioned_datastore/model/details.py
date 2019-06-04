import json

from sqlalchemy import Column, Table, BigInteger, UnicodeText

from ckan.model import meta, DomainObject

# this table stores general details about each version of each resource. Currently it only stores
# the column names and order.
datastore_resource_details_table = Table(
    u'versioned_datastore_resource_details',
    meta.metadata,
    Column(u'id', BigInteger, primary_key=True),
    Column(u'resource_id', UnicodeText, nullable=False, index=True),
    # the version this operation is creating (for ingestion this means the version it's adding to
    # mongo, for indexing this means the version it's pulling from mongo and putting into
    # elasticsearch)
    Column(u'version', BigInteger, nullable=False),
    # the detailed operation breakdown returned by eevee
    Column(u'columns', UnicodeText, nullable=False),
)


class DatastoreResourceDetails(DomainObject):
    '''
    Object for holding datastore resource details, currently just the columns at each version.
    '''

    def get_columns(self, validate=True):
        '''
        Retrieve the columns contained in this resource's version.

        :param validate: if True (the default) then fullstops are replaced with underscores before
                         returning the list of columns and any falsey columns (empty strings, Nones)
                         are removed
        :return: a list of column names in the order they were in the original data source
        '''
        columns = []
        for column in json.loads(self.columns):
            if validate:
                if not column:
                    continue
                column = column.replace(u'.', u'_')
            columns.append(column)
        return columns


meta.mapper(DatastoreResourceDetails, datastore_resource_details_table)
