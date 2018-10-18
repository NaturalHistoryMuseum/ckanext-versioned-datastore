from sqlalchemy import desc

from ckan import model

from ckanext.versioned_datastore.model.stats import ImportStats


def create_stats(resource_id):
    stats = ImportStats(resource_id=resource_id, ingest_count=0, index_count=0)
    stats.add()
    stats.commit()
    return stats.id


def finish_ingestion(stats_id, ingestion_stats):
    update = {
        ImportStats.ingest_in_progress: False,
        ImportStats.ingest_duration: ingestion_stats[u'duration'],
        ImportStats.ingest_start_time: ingestion_stats[u'start'],
        ImportStats.ingest_end_time: ingestion_stats[u'end'],
        ImportStats.ingest_operations: ingestion_stats[u'operations'],
    }
    model.Session.query(ImportStats).filter(ImportStats.id == stats_id).update(update)
    model.Session.commit()


def finish_indexing(stats_id, indexing_stats):
    update = {
        ImportStats.index_in_progress: False,
        ImportStats.index_duration: indexing_stats[u'duration'],
        ImportStats.index_start_time: indexing_stats[u'start'],
        ImportStats.index_end_time: indexing_stats[u'end'],
        ImportStats.index_operations: indexing_stats[u'operations'],
    }
    model.Session.query(ImportStats).filter(ImportStats.id == stats_id).update(update)
    model.Session.commit()


def ingestion_monitor(stats_id):
    def monitor(count, _record):
        update = {
            ImportStats.ingest_in_progress: True,
            ImportStats.ingest_count: count,
        }
        model.Session.query(ImportStats).filter(ImportStats.id == stats_id).update(update)
        model.Session.commit()

    return monitor


def indexing_monitor(stats_id):
    def monitor(_percentage, count, _total):
        update = {
            ImportStats.index_in_progress: True,
            ImportStats.index_count: count,
        }
        model.Session.query(ImportStats).filter(ImportStats.id == stats_id).update(update)
        model.Session.commit()

    return monitor


def get_latest_stats(resource_id):
    return model.Session.query(ImportStats).filter(
        ImportStats.resource_id == resource_id).order_by(desc(ImportStats.id)).first()
