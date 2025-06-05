from pathlib import Path
from typing import List, Optional, Tuple

from ckan.lib import search
from rq.job import Job
from splitgill.indexing.syncing import BulkOptions
from splitgill.model import IngestResult, Record

from ckanext.versioned_datastore.lib.importing.details import (
    create_details,
    get_last_file_hash,
)
from ckanext.versioned_datastore.lib.importing.ingest import (
    download_resource_data,
    iter_records,
)
from ckanext.versioned_datastore.lib.importing.readers import choose_reader_for_resource
from ckanext.versioned_datastore.lib.tasks import Task
from ckanext.versioned_datastore.lib.utils import (
    ReadOnlyResourceException,
    get_database,
    is_resource_read_only,
)
from ckanext.versioned_datastore.model.stats import INDEX, INGEST, PREP, ImportStats


def queue_ingest(
    resource: dict,
    replace: bool,
    api_key: str,
    records: Optional[List[dict]] = None,
) -> Tuple[Job, Job]:
    """
    Queues a new ingest job on the importing queue for the given resource.

    :param resource: the resource dict
    :param replace: whether any existing records should be deleted before adding the new
        records.
    :param api_key: the user's API key to use for file access (this is needed for
        private datasets)
    :param records: optional list of dicts to ingest instead of the file/URL on the
        resource
    :returns: a 2-tuple of the created ingest job and sync job which is dependent on the
        ingest job
    """
    if is_resource_read_only(resource['id']):
        raise ReadOnlyResourceException('This resource has been marked as read only')

    # create the ingest task first
    ingest_task = IngestResourceTask(resource, replace, api_key, records)
    ingest_job = ingest_task.queue()

    # then create a sync task dependent on the ingest task
    sync_task = SyncResourceTask(resource)
    sync_job = sync_task.queue(depends_on=ingest_job)

    return ingest_job, sync_job


def queue_delete(resource: dict) -> Tuple[Job, Job]:
    """
    Queues a new delete job for the given resource to remove all records. This is a
    versioned delete, so in reality all the data is maintained, just the latest versions
    of all records are set to empty.

    :param resource: the resource dict
    :returns: a 2-tuple of the created ingest job and sync job which is dependent on the
        ingest job
    """
    if is_resource_read_only(resource['id']):
        raise ReadOnlyResourceException('This resource has been marked as read only')

    # create the delete task first
    ingest_task = DeleteResourceTask(resource)
    delete_job = ingest_task.queue()

    # then create a sync task dependent on the delete task
    sync_task = SyncResourceTask(resource)
    sync_job = sync_task.queue(depends_on=delete_job)

    return delete_job, sync_job


def queue_sync(resource: dict, full: bool = False) -> Job:
    """
    Queues a sync job to synchronise any changes in MongoDB for this resource with
    Elasticsearch.

    :param resource: the resource dict
    :param full: whether to completely resync the Elasticsearch data with MongoDB or
        just sync the changes (default: False, just sync the changes)
    :returns: the queued sync job
    """
    if is_resource_read_only(resource['id']):
        raise ReadOnlyResourceException('This resource has been marked as read only')

    return SyncResourceTask(resource, full).queue()


def get_dupe_message(file_hash: str) -> str:
    """
    Returns an error message to be used for duplicate resource ingestions where nothing
    needs to be done. No error has really occurred so raising an exception feels like a
    bad way to deal with this, hence this str generator.

    :param file_hash: the file's hash
    :returns: a str to be stored as the stat's "error"
    """
    return f'This file has been ingested before, ignoring [hash: {file_hash}]'


class IngestResourceTask(Task):
    """
    Class representing a task to ingest a resource from a file/URL or from a given list
    of record dicts.
    """

    def __init__(
        self,
        resource: dict,
        replace: bool,
        api_key: str,
        records: Optional[List[dict]] = None,
    ):
        """
        :param resource: the resource dict
        :param replace: whether to replace all existing records with the records that
                        will be ingested or not
        :param api_key: the user's API key to use for file access (this is needed for
                        private datasets)
        :param records: optional list of dicts to ingest instead of the resource's
                        file/URL
        """
        self.resource = resource
        self.replace = replace
        self.api_key = api_key
        self.records = records

        # create a meaningful title
        if records is not None:
            data_info = f'{len(records)} records'
        else:
            data_info = 'data from file/url'
        title = f'Ingest for {self.resource_id} of {data_info} [replace: {replace}]'
        super().__init__('importing', title)

    @staticmethod
    def result_to_dict(result: IngestResult) -> dict:
        """
        Simple helper to convert an IngestResult object to a dict.

        :param result: the IngestResult object
        :returns: a dict
        """
        return {
            'inserted': result.inserted,
            'deleted': result.deleted,
            'updated': result.updated,
        }

    @property
    def resource_id(self) -> str:
        """
        :returns: the ID of the resource this ingest is operating on
        """
        return self.resource['id']

    def run(self, tmpdir: Path):
        """
        Performs the ingestion.

        :param tmpdir: a dir to use for temporary storage
        """
        # do the prep stage, this involves downloading the data
        with ImportStats.track(self.resource_id, PREP) as stats:
            if self.records is None:
                source = tmpdir / 'source'
                file_hash = download_resource_data(self.resource, source, self.api_key)
                last_hash = get_last_file_hash(self.resource_id)
                if file_hash == last_hash:
                    stats.update(error=get_dupe_message(file_hash))
                    self.log.info(get_dupe_message(file_hash))
                    return
            else:
                source = self.records
                file_hash = None

            reader = choose_reader_for_resource(self.resource, source)
            self.log.info(f'Using reader {reader.get_name()}')
            stats.update(count=reader.get_count())

        with ImportStats.track(self.resource_id, INGEST) as stats:
            database = get_database(self.resource_id)
            try:
                operations = {}
                if self.replace and database.has_data():
                    # to do a replace we delete everything before doing the ingest
                    replace_result = database.ingest(
                        (
                            Record.delete(record.id)
                            for record in database.iter_records()
                        ),
                        commit=False,
                    )
                    operations['replace'] = self.result_to_dict(replace_result)
                ingest_result = database.ingest(
                    iter_records(reader.read(), stats), commit=False
                )
                operations['ingest'] = self.result_to_dict(ingest_result)
                stats.update(
                    count=database.data_collection.count_documents({'version': None}),
                    operations=operations,
                )
            except:
                self.log.exception('Error while ingesting data, rolling back')
                database.rollback_records()
                raise

            version = database.commit()
            if version is None:
                self.log.info('No changes detected for data')
                stats.update(count=0)
                return
            else:
                stats.update(version=version)
                self.log.info(f'Ingested new data with version {version}')

            try:
                create_details(
                    self.resource_id, version, reader.get_fields(), file_hash
                )
            except Exception as e:
                self.log.warning(
                    f'Failed to create DatastoreResourceDetails due to {e}'
                )

        self.log.info('Finished ingesting')


class SyncResourceTask(Task):
    def __init__(
        self,
        resource: dict,
        full: bool = False,
    ):
        self.resource = resource
        self.full = full
        super().__init__('importing', f'Sync of {self.resource_id} [full: {self.full}]')

    @property
    def resource_id(self) -> str:
        return self.resource['id']

    def run(self, tmpdir: Path):
        with ImportStats.track(self.resource_id, INDEX) as stats:
            database = get_database(self.resource_id)
            index_version = database.get_elasticsearch_version()
            data_version = database.get_committed_version()
            # indicate that we've started doing something
            stats.update(count=0)

            if not self.full and index_version == data_version:
                # nothing to do
                self.log.info('Elasticsearch is already in sync, nothing to do')
            else:
                # work out how many records will be affected by the sync
                if self.full or index_version is None:
                    count = database.data_collection.count_documents({})
                else:
                    count = database.data_collection.count_documents(
                        {'version': {'$gt': index_version}}
                    )

                # use fairly modest values for syncing
                sync_options = BulkOptions(100, 2, 3)
                # do the sync and log/save info
                result = database.sync(bulk_options=sync_options, resync=self.full)
                stats.update(
                    operations={'deleted': result.deleted, 'indexed': result.indexed},
                    count=count,
                    version=database.get_elasticsearch_version(),
                )
                self.log.info(
                    f'Finished, indexed: {result.indexed}, deleted: {result.deleted}'
                )

        # refresh the data about this package in the solr search index to ensure that
        # the datastore_active flag is set correctly. The flag is actually set in the
        # plugin via a before_show resource hook so asking CKAN to refresh the package
        # will force it to rebuild the resource dict and thus call before_show and get
        # datastore_active set
        search.rebuild(package_id=self.resource['package_id'])


class DeleteResourceTask(Task):
    def __init__(self, resource: dict):
        self.resource = resource
        super().__init__('importing', f'Delete {self.resource_id}')

    @property
    def resource_id(self) -> str:
        return self.resource['id']

    def run(self, tmpdir: Path):
        with ImportStats.track(self.resource_id, INGEST) as stats:
            database = get_database(self.resource_id)
            result = database.ingest(
                (Record.delete(record.id) for record in database.iter_records()),
                commit=False,
            )
            count = database.data_collection.count_documents({'version': None})
            version = database.commit()
            if version is None:
                self.log.info('No changes detected for data or options')
                stats.update(count=0)
            else:
                self.log.info(f'Deleted all records at {version}')
                stats.update(
                    version=version, count=count, operations=result_to_dict(result)
                )
