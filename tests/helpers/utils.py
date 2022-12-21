from collections import namedtuple
from datetime import datetime as dt


def sync_enqueue_job(job_func, args=None, kwargs=None, *queue_args, **queue_kwargs):
    args = args or []
    kwargs = kwargs or {}
    job_func(*args, **kwargs)
    Job = namedtuple('Job', ['enqueued_at', 'id'])
    return Job(enqueued_at=dt.now(), id=1)
