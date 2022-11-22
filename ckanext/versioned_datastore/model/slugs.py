from datetime import datetime

from ckan.model import meta, DomainObject
from ckan.model.types import make_uuid
from sqlalchemy import (
    Column,
    Table,
    BigInteger,
    UnicodeText,
    UniqueConstraint,
    DateTime,
    or_,
)
from sqlalchemy.dialects.postgresql import JSONB

# this table stores query slugs
datastore_slugs_table = Table(
    'versioned_datastore_slugs',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column('query_hash', UnicodeText, nullable=False, index=True, unique=True),
    Column('query', JSONB, nullable=False),
    Column('query_version', UnicodeText, nullable=False),
    Column('version', BigInteger, nullable=True),
    Column('resource_ids', JSONB, nullable=True),
    Column('resource_ids_and_versions', JSONB, nullable=True),
    Column('pretty_slug', UnicodeText, nullable=True, index=True, unique=True),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('reserved_pretty_slug', UnicodeText, nullable=True, index=True, unique=True),
    UniqueConstraint('id', 'pretty_slug', 'reserved_pretty_slug'),
)


class DatastoreSlug(DomainObject):
    """
    Object for a datastore slug row.
    """

    def get_slug_string(self):
        """
        Returns the slug string to be used for this slug. This will be the first non-
        None value from the following attributes in this order: reserved_pretty_slug,
        pretty_slug or id.

        :return: the slug string
        """
        if self.reserved_pretty_slug is not None:
            return self.reserved_pretty_slug
        elif self.pretty_slug is not None:
            return self.pretty_slug
        else:
            return self.id

    @staticmethod
    def on_slug(slug_string):
        """
        Returns an or query that can be used to find the slug in the database with the
        passed slug string.

        :param slug_string: the slug string
        :return: an or filter
        """
        return or_(
            DatastoreSlug.id == slug_string,
            DatastoreSlug.pretty_slug == slug_string,
            DatastoreSlug.reserved_pretty_slug == slug_string,
        )


meta.mapper(DatastoreSlug, datastore_slugs_table)
