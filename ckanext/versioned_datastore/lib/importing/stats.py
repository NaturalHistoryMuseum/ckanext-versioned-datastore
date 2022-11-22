from traceback import format_exception_only

from ckan import model
from datetime import datetime
from sqlalchemy import desc

from ...model.stats import ImportStats

PREP = 'prep'
INGEST = 'ingest'
INDEX = 'index'
ALL_TYPES = [PREP, INDEX, INGEST]


def start_operation(resource_id, import_type, version, start=None):
    """
    Creates an ImportStats instance, saves it to the database and returns the database
    id of the newly created object.

    :param resource_id: the id of the resource being worked on
    :param import_type: the type of import operation being undertaken
    :param version: the version of the data
    :param start: the datetime when this operation was started (optional, if None current time will
                  be used)
    :return: the database id of the saved ImportStats object
    """
    if start is None:
        start = datetime.now()
    stats = ImportStats(
        resource_id=resource_id,
        type=import_type,
        version=version,
        in_progress=True,
        start=start,
    )
    stats.add()
    stats.commit()
    return stats.id


def update_stats(stats_id, update):
    """
    Update the ImportStats object with the given database id with the given update dict.
    The update dict will be passed directly to SQLAlchemy.

    :param stats_id: the database id of the object to update
    :param update: a dict of updates to apply
    """
    model.Session.query(ImportStats).filter(ImportStats.id == stats_id).update(update)
    model.Session.commit()


def finish_operation(stats_id, total, stats=None):
    """
    Update the ImportStats object with the given id to indicate that the operation is
    complete.

    :param stats_id: the database id of the object to finish
    :param total: the total number of records affected by this operation
    :param stats: the stats dict returned by the operation (optional)
    """
    if stats is None:
        start = model.Session.query(ImportStats).get(stats_id).start
        end = datetime.now()
        stats = {
            'duration': (end - start).total_seconds(),
            'start': start,
            'end': end,
            'operations': {},
        }
    update_stats(
        stats_id,
        {
            ImportStats.in_progress: False,
            ImportStats.count: total,
            ImportStats.duration: stats['duration'],
            ImportStats.start: stats['start'],
            ImportStats.end: stats['end'],
            ImportStats.operations: stats['operations'],
        },
    )


def monitor_ingestion(stats_id, ingester):
    """
    Adds monitoring functions to the ingester and updates the ImportStats (with the
    given database id) when updates come through.

    :param stats_id: the database id of the object to update
    :param ingester: the Ingester object to monitor
    """

    @ingester.totals_signal.connect_via(ingester)
    def on_ingest(_sender, total, inserted, updated):
        # this function is called each time a batch of records is ingested from the feeder into
        # mongo. This is done in batches and therefore we don't have to limit the frequency of our
        # database updates
        update_stats(
            stats_id,
            {
                ImportStats.in_progress: True,
                ImportStats.count: total,
            },
        )

    @ingester.finish_signal.connect_via(ingester)
    def on_finish(_sender, total, inserted, updated, stats):
        # this function is called when the ingestion operation completes
        finish_operation(stats_id, total, stats)


def monitor_indexing(stats_id, indexer, update_frequency=1000):
    """
    Adds monitoring functions to the indexer and updates the ImportStats (with the given
    database id) when updates come through.

    :param stats_id: the database id of the object to update
    :param indexer: the Indexer object to monitor
    :param update_frequency: the frequency with which to update the ImportStats. Setting this too
                             low will cause the database written to a lot which could cause
                             performance issues.
    """

    @indexer.index_signal.connect_via(indexer)
    def update_progress(_sender, indexing_stats, **kwargs):
        # this function is called each time a record is queued to be indexed into elasticsearch.
        # This means it is called a lot and therefore needs a barrier preventing it from hammering
        # the database, hence this modulo calculation
        if indexing_stats.document_count % update_frequency == 0:
            update_stats(
                stats_id,
                {
                    ImportStats.in_progress: True,
                    ImportStats.count: indexing_stats.document_count,
                },
            )

    @indexer.finish_signal.connect_via(indexer)
    def finish(_sender, indexing_stats, stats):
        # this function is called when the indexing operation completes
        finish_operation(stats_id, indexing_stats.document_count, stats)


def mark_error(stats_id, error):
    """
    Marks the ImportStats object with the given database id as having finished with an
    error. Just the error message is stored against the ImportStats object.
    "in_progress", "duration" and "end" are also updated.

    :param stats_id: the database id of the object to update
    :param error: the exception object
    """
    start = model.Session.query(ImportStats).get(stats_id).start
    end = datetime.now()
    update_stats(
        stats_id,
        {
            ImportStats.in_progress: False,
            ImportStats.duration: (end - start).total_seconds(),
            ImportStats.end: end,
            ImportStats.error: str(
                format_exception_only(type(error), error)[-1].strip()
            ),
        },
    )


def get_all_stats(resource_id):
    """
    Retrieves and returns all the ImportStats from the database associated with the
    given resource. They are ordered by ID descending which will result in the newest
    results coming back first.

    :param resource_id: the id of the resource
    :return: a Query object which can be iterated over to retrieve all the results
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
    :return: an ImportStats object or None
    """
    return (
        model.Session.query(ImportStats)
        .filter(ImportStats.resource_id == resource_id)
        .filter(ImportStats.type == INGEST)
        .order_by(ImportStats.version.desc())
        .first()
    )
