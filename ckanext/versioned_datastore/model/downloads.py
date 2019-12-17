from datetime import datetime

from ckan.model import meta, DomainObject
from ckan.model.types import make_uuid
from sqlalchemy import Column, Table, BigInteger, UnicodeText, DateTime
from sqlalchemy.dialects.postgresql import JSONB

state_complete = u'complete'
state_failed = u'failed'
state_processing = u'processing'
state_zipping = u'zipping'


# this table stores query slugs
datastore_downloads_table = Table(
    u'versioned_datastore_downloads',
    meta.metadata,
    Column(u'id', UnicodeText, primary_key=True, default=make_uuid),
    Column(u'query_hash', UnicodeText, nullable=False, index=True),
    Column(u'query', JSONB, nullable=False),
    Column(u'query_version', UnicodeText, nullable=False),
    Column(u'resource_ids_and_versions', JSONB, nullable=False),
    Column(u'created', DateTime, nullable=False, default=datetime.utcnow),
    Column(u'total', BigInteger, nullable=True),
    Column(u'resource_totals', JSONB, nullable=True),
    Column(u'state', UnicodeText, nullable=True),
    Column(u'error', UnicodeText, nullable=True),
    Column(u'options', JSONB, nullable=True),
)


class DatastoreDownload(DomainObject):
    '''
    Object for a datastore download row.
    '''
    pass


meta.mapper(DatastoreDownload, datastore_downloads_table)
