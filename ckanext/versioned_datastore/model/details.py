import json

from ckan.model import meta, DomainObject
from sqlalchemy import Column, Table, BigInteger, UnicodeText

# this table stores general details about each version of each resource. Currently it only stores
# the column names and order.
datastore_resource_details_table = Table(
    'versioned_datastore_resource_details',
    meta.metadata,
    Column('id', BigInteger, primary_key=True),
    Column('resource_id', UnicodeText, nullable=False, index=True),
    # the version this operation is creating (for ingestion this means the version it's adding to
    # mongo, for indexing this means the version it's pulling from mongo and putting into
    # elasticsearch)
    Column('version', BigInteger, nullable=False),
    # the detailed operation breakdown returned by splitgill
    Column('columns', UnicodeText, nullable=False),
    # a hash of the ingested file - can be None and will be if records are directly added using the
    # API
    Column('file_hash', UnicodeText),
)


class DatastoreResourceDetails(DomainObject):
    """
    Object for holding datastore resource details, currently just the columns at each
    version.
    """

    def get_columns(self, validate=True):
        """
        Retrieve the columns contained in this resource's version.

        :param validate: if True (the default) then fullstops are replaced with underscores before
                         returning the list of columns and any falsey columns (empty strings, Nones)
                         are removed
        :return: a list of column names in the order they were in the original data source
        """
        columns = []
        for column in json.loads(self.columns):
            if validate:
                if not column:
                    continue
                column = column.replace('.', '_')
            columns.append(column)
        return columns


meta.mapper(DatastoreResourceDetails, datastore_resource_details_table)
