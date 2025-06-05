from contextlib import contextmanager
from datetime import datetime, timezone
from traceback import format_exception_only
from typing import Optional, Union

from ckan import model
from ckan.model import DomainObject, Session, meta
from ckan.model.types import JsonDictType
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Table,
    UnicodeText,
    desc,
)
from sqlalchemy.exc import InvalidRequestError

# options for the ImportStats.type field
PREP = 'prep'
INGEST = 'ingest'
INDEX = 'index'
ALL_TYPES = [PREP, INDEX, INGEST]

# this table stores general "statistics" about the ingest and index events that occur on
# resources. It is also used to figure out what versions have been ingested and to a
# certain extent indexed and it's therefore pretty important (it is used to avoid
# ingesting older versions for example).
import_stats_table = Table(
    'versioned_datastore_import_stats',
    meta.metadata,
    Column('id', BigInteger, primary_key=True),
    Column('resource_id', UnicodeText, nullable=False, index=True),
    # the type of operation
    Column('type', UnicodeText, nullable=False),
    # the version this operation created or is working on, this will be null for ingests
    Column('version', BigInteger),
    # the start datetime of the operation
    Column('start', DateTime),
    # the end datetime of the operation
    Column('end', DateTime),
    # how long the operation took in seconds
    Column('duration', Float),
    # whether the operation is in progress or whether it has completed
    Column('in_progress', Boolean),
    # if there was an error, this column is populated with the details
    Column('error', UnicodeText),
    # the number of records handled during the operation
    Column('count', BigInteger),
    # the detailed operation breakdown returned by splitgill
    Column('operations', JsonDictType),
)


class ImportStats(DomainObject):
    """
    Object for holding resource import stats.
    """

    @classmethod
    def get(cls, stats_id: int) -> Optional['ImportStats']:
        return Session.query(cls).get(stats_id)

    def update(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self, field, value)
        try:
            self.save()
        except InvalidRequestError:
            self.commit()

    @classmethod
    def begin(cls, resource_id: str, stat_type: str):
        stats = cls(
            resource_id=resource_id,
            type=stat_type,
            in_progress=True,
            start=datetime.now(timezone.utc),
        )
        stats.save()
        return stats

    @classmethod
    @contextmanager
    def track(cls, resource_id: str, stat_type: str):
        stats = cls.begin(resource_id, stat_type)
        try:
            yield stats
        except Exception as e:
            stats.mark_error(e)
            raise
        finally:
            stats.finish()

    def mark_error(self, error: Union[Exception, str]):
        """
        Marks the ImportStats object as having finished with an error. Just the error
        message is stored against the ImportStats object. "in_progress", "duration" and
        "end" are also updated.

        :param error: the exception object or string message
        """
        if isinstance(error, Exception):
            str_error = str(format_exception_only(type(error), error)[-1].strip())
        else:
            str_error = error
        self.update(error=str_error)
        self.finish()

    def finish(self):
        end = datetime.now(timezone.utc)
        self.update(
            in_progress=False, end=end, duration=(end - self.start).total_seconds()
        )


meta.mapper(ImportStats, import_stats_table)


def get_all_stats(resource_id):
    """
    Retrieves and returns all the ImportStats from the database associated with the
    given resource. They are ordered by ID descending which will result in the newest
    results coming back first.

    :param resource_id: the id of the resource
    :returns: a Query object which can be iterated over to retrieve all the results
    """
    return list(
        model.Session.query(ImportStats)
        .filter(ImportStats.resource_id == resource_id)
        .order_by(desc(ImportStats.id))
    )


def get_last_ingest(resource_id):
    """
    Retrieve the last ingest stat object from the database, or None if there aren't any.

    :param resource_id: the resource id
    :returns: an ImportStats object or None
    """
    return (
        model.Session.query(ImportStats)
        .filter(ImportStats.resource_id == resource_id)
        .filter(ImportStats.type == INGEST)
        .order_by(ImportStats.version.desc())
        .first()
    )
