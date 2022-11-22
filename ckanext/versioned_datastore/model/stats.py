from ckan.model import meta, DomainObject
from ckan.model.types import JsonDictType
from sqlalchemy import Column, DateTime, Float, Boolean, Table, BigInteger, UnicodeText

# this table stores general statistics about the ingest and index events that occur on resources. It
# is also used to figure out what versions have been ingested and to a certain extent indexed and
# it's therefore pretty important (it is used to avoid ingesting older versions for example).
import_stats_table = Table(
    'versioned_datastore_import_stats',
    meta.metadata,
    Column('id', BigInteger, primary_key=True),
    Column('resource_id', UnicodeText, nullable=False, index=True),
    # the type of operation, either ingest or index
    Column('type', UnicodeText, nullable=False),
    # the version this operation is creating (for ingestion this means the version it's adding to
    # mongo, for indexing this means the version it's pulling from mongo and putting into
    # elasticsearch)
    Column('version', BigInteger, nullable=False),
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

    pass


meta.mapper(ImportStats, import_stats_table)
