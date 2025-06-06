import json
from datetime import date

from ckan.plugins import toolkit

from ckanext.versioned_datastore.lib.common import ALL_FORMATS
from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.query.slugs.slugs import create_nav_slug
from ckanext.versioned_datastore.model import stats


def is_duplicate_ingestion(stat):
    """
    Detects whether the error message on this ImportStats object is a
    DuplicateDataSource error message or not. This is based on the error containing the
    phrase "this file has been ingested before" and is therefore a quite fragile, but
    it's only for UI purposes so it's not he end of the world if it produces a few false
    positives/negatives.

    :param stat: the ImportStats object
    :returns: True if the error on this stat is a duplicate ingestion error, False if
        not
    """
    return stat.error and 'this file has been ingested before' in stat.error.lower()


def get_human_duration(stat):
    """
    Get the duration on the passed ImportStats object in a sensible human readable
    format. The duration on stats objects is in seconds and therefore is great for small
    values but horrendous if it took 20 minutes. The output from this function is a
    string with either the number of seconds (to 2 decimal places), the number of
    minutes (to 0 decimal places) or the number of hours (to 0 decimal places).

    :param stat: the ImportStats object
    :returns: a nicely formatted duration string
    """
    if stat.duration < 60:
        return toolkit._(f'{stat.duration:.2f} seconds')
    elif stat.duration < 60 * 60:
        return toolkit._(f'{stat.duration / 60:.0f} minutes')
    else:
        return toolkit._(f'{stat.duration / (60 * 60):.0f} hours')


def get_stat_icon(stat):
    """
    Returns the fontawesome icon class(-es) to be used for the given ImportStats object.
    The return value is based on the type of stat and other factors like whether the
    operation the stat represents is still in progress.

    :param stat: the ImportStats object
    :returns: the fontawesome icon classes to use, as a string
    """
    if stat.in_progress:
        # a spinner, that spins
        return 'fa-spinner fa-pulse'
    if stat.error:
        if is_duplicate_ingestion(stat):
            # we don't want this to look like an error
            return 'fa-copy'
        return 'fa-exclamation'

    if stat.type == stats.INGEST:
        return 'fa-tasks'
    if stat.type == stats.INDEX:
        return 'fa-search'
    if stat.type == stats.PREP:
        return 'fa-cogs'
    # shouldn't get here, just use some default tick thing
    return 'fa-check'


def get_stat_activity_class(stat):
    """
    Returns the activity css class to use for the given ImportStats object. The return
    value is a string css class which either matches one of the activity item classes
    from core ckan or matches one of the custom ones in this extension's css.

    :param stat: the ImportStats object
    :returns: a string
    """
    if stat.in_progress:
        return 'in_progress'
    if stat.error:
        if is_duplicate_ingestion(stat):
            return 'duplicate'
        return 'failure'
    return stat.type


def get_stat_title(stat):
    """
    Returns the title to use for the activity item created for the given ImportStats
    object. This is based on the stat's type.

    :param stat: the ImportStats object
    :returns: the title for the activity item as a unicode string
    """
    if stat.type == stats.INGEST:
        return toolkit._('Ingested new resource data')
    if stat.type == stats.INDEX:
        return toolkit._('Updated search index with resource data')
    if stat.type == stats.PREP:
        return toolkit._('Validated and prepared the data for ingestion')
    return stat.type


def get_available_formats():
    """
    Simply returns all the formats that we can ingest.

    :returns: a list of formats
    """
    return ALL_FORMATS


def pretty_print_json(json_string):
    """
    Does what you'd expect really.

    :param json_string: a json string
    :returns: a string of pretty json
    """
    return json.dumps(json_string, sort_keys=True, indent=2)


def get_version_date(version):
    """
    Returns a date object from a version number.

    :param version: a resource/record version number (i.e. a timestamp in ms)
    :returns: a date object
    """
    return date.fromtimestamp(int(version) / 1000)


def latest_item_version(resource_id, record_id=None):
    """
    Returns the most recent version for the given resource or record.

    :param resource_id: the id of the resource to search in
    :param record_id: optional; a record id to search for instead
    :returns: if record id is provided, the latest record version, else the latest
        resource version
    """
    if record_id:
        action = 'vds_version_record'
        data_dict = {'resource_id': resource_id, 'record_id': record_id}
    else:
        action = 'vds_version_resource'
        data_dict = {'resource_id': resource_id}

    result = toolkit.get_action(action)({}, data_dict)

    if record_id:
        # we get back a list of versions in ascending order
        return result[-1]
    else:
        # we get back a dict of versions and counts
        return max(result.keys())


def nav_slug(query=None, version=None, resource_ids=None):
    """
    Just a helper proxy for create_nav_slug.
    """
    is_new, slug = create_nav_slug(SchemaQuery(resource_ids, version, query))
    return slug.get_slug_string()
