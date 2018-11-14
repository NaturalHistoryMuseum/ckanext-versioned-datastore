from datetime import datetime

from sqlalchemy import desc

from ckan import model
from ckanext.versioned_datastore.model.stats import ImportStats


INGEST = u'ingest'
INDEX = u'index'


def start_operation(resource_id, import_type, version, start):
    stats = ImportStats(resource_id=resource_id, type=import_type, version=version,
                        in_progress=True, start=start)
    stats.add()
    stats.commit()
    return stats.id


def update_stats(stats_id, update):
    model.Session.query(ImportStats).filter(ImportStats.id == stats_id).update(update)
    model.Session.commit()


def finish_operation(stats_id, stats):
    update_stats(stats_id, {
        ImportStats.in_progress: False,
        ImportStats.duration: stats[u'duration'],
        ImportStats.start: stats[u'start'],
        ImportStats.end: stats[u'end'],
        ImportStats.operations: stats[u'operations'],
    })


def ingestion_monitor(stats_id):
    def monitor(count, _record):
        update_stats(stats_id, {
            ImportStats.in_progress: True,
            ImportStats.count: count,
        })

    return monitor


def indexing_monitor(stats_id):
    def monitor(_percentage, count, _total):
        update_stats(stats_id, {
            ImportStats.in_progress: True,
            ImportStats.count: count,
        })

    return monitor


def mark_error(stats_id, error):
    start = model.Session.query(ImportStats).get(stats_id).start
    end = datetime.now()
    update_stats(stats_id, {
        ImportStats.in_progress: False,
        ImportStats.duration: (end - start).total_seconds(),
        ImportStats.end: end,
        ImportStats.error: error.message,
    })


def get_all_stats(resource_id):
    return model.Session.query(ImportStats).filter(
        ImportStats.resource_id == resource_id).order_by(desc(ImportStats.id))
