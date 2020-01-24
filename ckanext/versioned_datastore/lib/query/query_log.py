import json
import logging
from logging.handlers import RotatingFileHandler

from ckan.plugins import toolkit
from contextlib2 import suppress

config_base = u'ckanext.versioned_datastore.query_log_{}'
# default enabled to False
is_enabled = toolkit.asbool(toolkit.config.get(config_base.format(u'enabled'), False))
# default path to None which, if the enabled flag is True, will direct everything to the console log
path = toolkit.config.get(config_base.format(u'path'), None)
# default the rotate at bytes value to 5MiB
rotate_at_bytes = toolkit.asint(toolkit.config.get(config_base.format(u'rotate_at_bytes'), 5242880))
# default the number of backup files to keep at 5
backup_count = toolkit.asint(toolkit.config.get(config_base.format(u'backup_count'), 5))


if is_enabled:
    logger = logging.getLogger(u'query-log')
    logger.setLevel(logging.INFO)
    if path:
        file_handler = RotatingFileHandler(path, encoding=u'utf-8', maxBytes=rotate_at_bytes,
                                           backupCount=backup_count)
        formatter = logging.Formatter(u'%(asctime)s: %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        # this stops the query log messages from coming out in the main console log
        logger.propagate = False
else:
    logger = None


def log_query(query, source):
    '''
    Call this to log a query dict to the query log file (if the config permits it). The query is
    simply json dumped. If query logging is disabled then nothing happens.

    :param query: the query dict
    :param source: the source of the query (e.g. multisearch or basicsearch)
    '''
    if is_enabled:
        # use suppress just to make sure nothing explodes whilst logging
        with suppress(Exception):
            logger.info(u'{}: {}'.format(source, json.dumps(query, sort_keys=True,
                                                            ensure_ascii=False)))
