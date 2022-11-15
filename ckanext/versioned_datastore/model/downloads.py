from datetime import datetime

from ckan.model import meta, DomainObject
from ckan.model.types import make_uuid
from sqlalchemy import Column, Table, BigInteger, UnicodeText, DateTime
from sqlalchemy.dialects.postgresql import JSONB

state_complete = 'complete'
state_failed = 'failed'
state_processing = 'processing'
state_zipping = 'zipping'

# this table stores query slugs
datastore_downloads_table = Table(
    'versioned_datastore_downloads',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column('query_hash', UnicodeText, nullable=False, index=True),
    Column('query', JSONB, nullable=False),
    Column('query_version', UnicodeText, nullable=False),
    Column('resource_ids_and_versions', JSONB, nullable=False),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('total', BigInteger, nullable=True),
    Column('resource_totals', JSONB, nullable=True),
    Column('state', UnicodeText, nullable=True),
    Column('error', UnicodeText, nullable=True),
    Column('options', JSONB, nullable=True),
)


class DatastoreDownload(DomainObject):
    """
    Object for a datastore download row.
    """

    pass


meta.mapper(DatastoreDownload, datastore_downloads_table)
