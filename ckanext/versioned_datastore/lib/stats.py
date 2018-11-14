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


def finish_operation(stats_id, total, stats):
    update_stats(stats_id, {
        ImportStats.in_progress: False,
        ImportStats.count: total,
        ImportStats.duration: stats[u'duration'],
        ImportStats.start: stats[u'start'],
        ImportStats.end: stats[u'end'],
        ImportStats.operations: stats[u'operations'],
    })


def monitor_ingestion(stats_id, ingester):
    @ingester.totals_signal.connect_via(ingester)
    def on_ingest(_sender, total, inserted, updated):
        update_stats(stats_id, {
            ImportStats.in_progress: True,
            ImportStats.count: total,
        })

    @ingester.finish_signal.connect_via(ingester)
    def on_finish(_sender, total, inserted, updated, stats):
        finish_operation(stats_id, total, stats)


def monitor_indexing(stats_id, indexer, update_frequency=1000):
    @indexer.index_signal.connect_via(indexer)
    def on_index(_sender, document_count, command_count, document_total):
        if document_count % update_frequency == 0:
            update_stats(stats_id, {
                ImportStats.in_progress: True,
                ImportStats.count: document_count,
            })

    @indexer.finish_signal.connect_via(indexer)
    def on_finish(_sender, document_count, command_count, stats):
        finish_operation(stats_id, document_count, stats)


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
