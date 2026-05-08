import json

from ckan.model import DomainObject, meta
from splitgill.diffing import prepare_field_name
from sqlalchemy import BigInteger, Column, Table, UnicodeText

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

    def get_columns(self, validate=None, skip_empty=True, fix_names=True):
        """
        Retrieve the columns contained in this resource's version.

        :param validate: for backwards compatibility; sets both skip_empty and fix_names
        :param skip_empty: if True, remove falsey (empty strings, Nones) columns
            (default True)
        :param fix_names: if True, change column names to match datastore/splitgill
            (default True)
        :returns: a list of column names in the order they were in the original data
            source
        """
        if validate is not None:
            skip_empty = validate
            fix_names = validate

        columns = []
        for column in json.loads(self.columns):
            if skip_empty and not column:
                continue
            if fix_names:
                column = prepare_field_name(column)
            columns.append(column)
        return columns


meta.mapper(DatastoreResourceDetails, datastore_resource_details_table)
