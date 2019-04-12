from sqlalchemy import Column, DateTime, Float, Boolean, Table, BigInteger, UnicodeText

from ckan.model import meta, DomainObject
from ckan.model.types import JsonDictType


# this table stores general statistics about the ingest and index events that occur on resources. It
# is also used to figure out what versions have been ingested and to a certain extent indexed and
# it's therefore pretty important (it is used to avoid ingesting older versions for example).
import_stats_table = Table(
    u'versioned_datastore_import_stats',
    meta.metadata,
    Column(u'id', BigInteger, primary_key=True),
    Column(u'resource_id', UnicodeText, nullable=False, index=True),
    # the type of operation, either ingest or index
    Column(u'type', UnicodeText, nullable=False),
    # the version this operation is creating (for ingestion this means the version it's adding to
    # mongo, for indexing this means the version it's pulling from mongo and putting into
    # elasticsearch)
    Column(u'version', BigInteger, nullable=False),
    # the start datetime of the operation
    Column(u'start', DateTime),
    # the end datetime of the operation
    Column(u'end', DateTime),
    # how long the operation took in seconds
    Column(u'duration', Float),
    # whether the operation is in progress or whether it has completed
    Column(u'in_progress', Boolean),
    # if there was an error, this column is populated with the details
    Column(u'error', UnicodeText),
    # the number of records handled during the operation
    Column(u'count', BigInteger),
    # the detailed operation breakdown returned by eevee
    Column(u'operations', JsonDictType),
)


class ImportStats(DomainObject):
    '''
    Object for holding resource import stats.
    '''
    pass


meta.mapper(ImportStats, import_stats_table)
