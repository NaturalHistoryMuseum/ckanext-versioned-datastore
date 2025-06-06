import abc
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Union

from cachetools import TTLCache, cached
from ckan.plugins import toolkit
from rq.job import Job

from ckanext.versioned_datastore.lib.utils import es_client


class Task(abc.ABC):
    """
    Abstract base class for VDS queued tasks, for example importing downloading data.
    """

    def __init__(
        self,
        queue_name: str,
        title: str,
        timeout: int = 3600,
    ):
        """
        :param queue_name: the name of the queue this task should run on
        :param title: the title of the task
        :param timeout: how long RQ should wait for this task to complete, in seconds,
                        before timing it out (defaults to 3600 which is 1 hour)
        """
        self.queue_name = queue_name
        self.title = title
        self.timeout = timeout
        self.log = logging.getLogger(__name__)

    @abc.abstractmethod
    def run(self, tmpdir: Path):
        """
        Method stub for subclasses to override. This is automatically called by start
        when a task begins after some setup is completed.

        :param tmpdir: a temporary directory which can be used by the task and will be
            cleaned up after this run method completes
        """
        ...

    def start(self):
        """
        Starts the task, performs some setup and then calls self.run with a temporary
        directory.
        """
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            self.log.info(f'Starting {self.title}')
            self.run(Path(tmp_dir_name))
        self.log.info(f'Finished {self.title}')

    def queue(
        self,
        depends_on: Optional[Union[Job, List[Job]]] = None,
        timeout: Optional[int] = None,
    ) -> Job:
        """
        Queues this task on the appropriate RQ queue.

        :param depends_on: a job, or list of jobs, which this task depends on. If given,
            this task will only start when the other jobs finish successfully. Check the
            RQ docs for more information.
        :param timeout: a timeout in seconds for this task. If provided this overrides
            the base timeout defined in the task, otherwise the default task's timeout
            is used.
        :returns: returns the result of calling CKAN's enqueue_job which will provide
            details about the queued job for this task
        """
        rq_kwargs = {'timeout': timeout or self.timeout}
        if depends_on:
            rq_kwargs['depends_on'] = depends_on

        return toolkit.enqueue_job(
            self.start,
            queue=self.queue_name,
            title=self.title,
            rq_kwargs=rq_kwargs,
        )


@cached(cache=TTLCache(maxsize=10, ttl=300))
def get_queue_length(queue_name):
    """
    This will only get the pending jobs in a queue, not any jobs that are currently
    processing.

    :param queue_name: the name of the queue to check, e.g. 'download'
    :returns: length of queue as int
    """
    queued_jobs = toolkit.get_action('job_list')(
        {'ignore_auth': True}, {'queues': [queue_name]}
    )
    return len(queued_jobs)


@cached(cache=TTLCache(maxsize=10, ttl=300))
def get_es_health():
    client = es_client()
    return {'ping': client.ping(), 'info': client.info()}
