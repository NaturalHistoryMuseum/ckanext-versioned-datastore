from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib import stats


def is_duplicate_ingestion(stat):
    '''
    Detects whether the error message on this ImportStats object is a DuplicateDataSource error
    message or not. This is based on the error containing the phrase "this file has been ingested
    before" and is therefore a quite fragile, but it's only for UI purposes so it's not he end of
    the world if it produces a few false positives/negatives.

    :param stat: the ImportStats object
    :return: True if the error on this stat is a duplicate ingestion error, False if not
    '''
    return stat.error and u'this file has been ingested before' in stat.error.lower()


def get_human_duration(stat):
    '''
    Get the duration on the passed ImportStats object in a sensible human readable format. The
    duration on stats objects is in seconds and therefore is great for small values but horrendous
    if it took 20 minutes. The output from this function is a string with either the number of
    seconds (to 2 decimal places), the number of minutes (to 0 decimal places) or the number of
    hours (to 0 decimal places).

    :param stat: the ImportStats object
    :return: a nicely formatted duration string
    '''
    if stat.duration < 60:
        return toolkit._(u'{:.2f} seconds'.format(stat.duration))
    elif stat.duration < 60 * 60:
        return toolkit._(u'{:.0f} minutes'.format(stat.duration / 60))
    else:
        return toolkit._(u'{:.0f} hours'.format(stat.duration / (60 * 60)))


def get_stat_icon(stat):
    '''
    Returns the fontawesome icon class(-es) to be used for the given ImportStats object. The return
    value is based on the type of stat and other factors like whether the operation the stat
    represents is still in progress.

    :param stat: the ImportStats object
    :return: the fontawesome icon classes to use, as a string
    '''
    if stat.in_progress:
        # a spinner, that spins
        return u'fa-spinner fa-pulse'
    if stat.error:
        if is_duplicate_ingestion(stat):
            # we don't want this to look like an error
            return u'fa-copy'
        return u'fa-exclamation'

    if stat.type == stats.INGEST:
        return u'fa-tasks'
    if stat.type == stats.INDEX:
        return u'fa-search'
    if stat.type == stats.PREP:
        return u'fa-cogs'
    # shouldn't get here, just use some default tick thing
    return u'fa-check'


def get_stat_activity_class(stat):
    '''
    Returns the activity css class to use for the given ImportStats object. The return value is a
    string css class which either matches one of the activity item classes from core ckan or matches
    one of the custom ones in this extension's css.

    :param stat: the ImportStats object
    :return: a string
    '''
    if stat.in_progress:
        return u'in_progress'
    if stat.error:
        if is_duplicate_ingestion(stat):
            return u'duplicate'
        return u'failure'
    return stat.type


def get_stat_title(stat):
    '''
    Returns the title to use for the activity item created for the given ImportStats object. This is
    based on the stat's type.

    :param stat: the ImportStats object
    :return: the title for the activity item as a unicode string
    '''
    if stat.type == stats.INGEST:
        return toolkit._(u'Ingested new resource data')
    if stat.type == stats.INDEX:
        return toolkit._(u'Updated search index with resource data')
    if stat.type == stats.PREP:
        return toolkit._(u'Validated and prepared the data for ingestion')
    return stat.type
