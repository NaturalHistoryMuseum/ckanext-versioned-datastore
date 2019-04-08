import json

from sqlalchemy import Column, Table, BigInteger, UnicodeText

from ckan.model import meta, DomainObject

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

    def get_columns(self):
        return json.loads(self.columns)


meta.mapper(DatastoreResourceDetails, datastore_resource_details_table)
