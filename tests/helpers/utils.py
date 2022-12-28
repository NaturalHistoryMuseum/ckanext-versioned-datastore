import itertools
from collections import namedtuple
from datetime import datetime as dt
from threading import Thread

next_id = itertools.count(start=1)
Job = namedtuple('Job', ['enqueued_at', 'id'])


def sync_enqueue_job(job_func, args=None, kwargs=None, *queue_args, **queue_kwargs):
    args = args or []
    kwargs = kwargs or {}
    job_func(*args, **kwargs)
    return Job(enqueued_at=dt.now(), id=1)


def sync_enqueue_job_thread(
    job_func, args=None, kwargs=None, *queue_args, **queue_kwargs
):
    args = args or []
    kwargs = kwargs or {}

    job_thread = Thread(target=job_func, args=args, kwargs=kwargs)
    job_thread.start()
    job_thread.join()

    return Job(enqueued_at=dt.now(), id=next(next_id))
