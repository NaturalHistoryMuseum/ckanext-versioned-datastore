from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Table

from ckan.model import meta, DomainObject
from ckan.model.types import JsonDictType


import_stats_table = Table('versioned_datastore_import_stats', meta.metadata,
                           Column(u'id', Integer, primary_key=True),
                           Column(u'resource_id', String, nullable=False),
                           Column(u'ingest_start_time', DateTime),
                           Column(u'ingest_end_time', DateTime),
                           Column(u'ingest_duration', Float),
                           Column(u'ingest_in_progress', Boolean),
                           Column(u'ingest_count', Integer),
                           Column(u'ingest_operations', JsonDictType),
                           Column(u'index_start_time', DateTime),
                           Column(u'index_end_time', DateTime),
                           Column(u'index_duration', Float),
                           Column(u'index_in_progress', Boolean),
                           Column(u'index_count', Integer),
                           Column(u'index_operations', JsonDictType))


class ImportStats(DomainObject):
    '''
    Object for holding resource import stats.
    '''
    pass


meta.mapper(ImportStats, import_stats_table)
