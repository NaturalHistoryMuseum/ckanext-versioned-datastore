from ckan.model import meta, DomainObject
from ckan.model.types import make_uuid
from datetime import datetime
from sqlalchemy import Column, Table, BigInteger, UnicodeText, UniqueConstraint, DateTime
from sqlalchemy.dialects.postgresql import JSONB

# this table stores query slugs
datastore_slugs_table = Table(
    u'versioned_datastore_slugs',
    meta.metadata,
    Column(u'id', UnicodeText, primary_key=True, default=make_uuid),
    Column(u'query_hash', UnicodeText, nullable=False, index=True, unique=True),
    Column(u'query', JSONB, nullable=False),
    Column(u'query_version', UnicodeText, nullable=False),
    Column(u'version', BigInteger, nullable=True),
    Column(u'resource_ids', JSONB, nullable=True),
    Column(u'resource_ids_and_versions', JSONB, nullable=True),
    Column(u'pretty_slug', UnicodeText, nullable=True, index=True, unique=True),
    Column(u'created', DateTime, nullable=False, default=datetime.utcnow),
    UniqueConstraint(u'id', u'pretty_slug')
)


class DatastoreSlug(DomainObject):
    '''
    Object for a datastore slug row.
    '''
    pass


meta.mapper(DatastoreSlug, datastore_slugs_table)
