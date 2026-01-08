import logging
import re
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, TypeVar

from beaker.cache import CacheManager, cache_region, cache_regions
from ckan import plugins
from ckan.plugins import get_plugin, toolkit
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from splitgill.manager import SplitgillClient, SplitgillDatabase

from ckanext.versioned_datastore.interfaces import (
    IVersionedDatastore,
    IVersionedDatastoreDownloads,
    IVersionedDatastoreQuerySchema,
)
from ckanext.versioned_datastore.lib import common

log = logging.getLogger(__name__)


def get_available_datastore_resources(
    ignore_auth: bool = False, user_id: str = ''
) -> Set[str]:
    """
    Simple wrapper around get_available_resources which provides a list of available
    datastore resources to the currently logged-in user.

    :param ignore_auth: whether to ignore authentication (default: False)
    :returns: a set of resource IDs
    """
    return get_available_resources(
        datastore_only=True, ignore_auth=ignore_auth, user_id=user_id
    )


def get_available_resources(
    datastore_only: bool, ignore_auth: bool = False, user_id: str = ''
) -> Set[str]:
    """
    Get a set of resource IDs that are available to the currently logged-in user and, if
    datastore_only is set to True, are datastore active. If no user is logged-in, all
    public datastore resource IDs are returned. The resource IDs are returned as a set
    to enable quick checking between a list of requested IDs and the list of available
    IDs.

    :param datastore_only: whether to only return resource IDs that are datastore active
    :param ignore_auth: whether to ignore authentication (default: False)
    :returns: a set of resource IDs
    """
    resource_ids = set()

    offset = 0
    action = toolkit.get_action('current_package_list_with_resources')

    while True:
        context = {}
        if ignore_auth:
            # unless ignore auth is passed, in which case pass that in the context
            context['ignore_auth'] = True
        else:
            context['user'] = user_id
        packages = action(context, {'offset': offset, 'limit': 100})
        if not packages:
            break
        for package in packages:
            # add the datastore active resources
            resource_ids.update(
                resource['id']
                for resource in package['resources']
                if not datastore_only or resource.get('datastore_active', False)
            )
        offset += len(packages)

    return resource_ids


@cache_region('vds', 'public_resources')
def get_public_resources() -> Dict[str, bool]:
    """
    Retrieves a list of public resources and whether they are present in the datastore.

    :returns: dict of publicly available resource IDs and whether they are available in
        the datastore
    """
    resource_ids = {}
    offset = 0
    action = toolkit.get_action('current_package_list_with_resources')
    while True:
        # do not ignore auth, do not provide a user
        context = {'ignore_auth': False, 'user': None}
        packages = action(context, {'offset': offset, 'limit': 100})
        if not packages:
            break
        for package in packages:
            for resource in package.get('resources', []):
                resource_ids[resource['id']] = is_datastore_resource(resource['id'])
        offset += len(packages)

    return resource_ids


@cache_region('vds', 'resource_fields')
def get_latest_resource_fields(resource_ids: List[str]) -> Dict[str, Dict[str, Dict]]:
    """
    Retrieves a list of fields available on the latest indices for the given resources.

    Does not do any authentication. Only call from within other authenticated functions.

    :param resource_ids: list of resource IDs
    :returns: dict of field names, the resources they're found in, and their details
        within those resources
    """
    fields = defaultdict(dict)
    for resource_id in resource_ids:
        database = get_database(resource_id)

        try:
            parsed_fields = database.get_field_names()
        except toolkit.NotFoundError:
            # temporary fix for splitgill#38 (so we can ignore unavailable resources)
            return fields

        for field in parsed_fields:
            fields[field.path][resource_id] = {
                'name': field.name,
                'path': field.path,
                'text': field.is_text,
                'keyword': field.is_keyword,
                'boolean': field.is_boolean,
                'date': field.is_date,
                'number': field.is_number,
                'geo': field.is_geo,
            }

    return fields


def get_database(resource_id: str) -> SplitgillDatabase:
    """
    Retrieves a SplitgillDatabase object for the given resource ID. If the
    SplitgillClient on the VDS plugin isn't yet configured, an exception is raised.

    :param resource_id: the resource's ID
    :returns: a SplitgillDatabase
    """
    name = get_sg_name(resource_id)
    return sg_client().get_database(name)


def sg_client() -> SplitgillClient:
    """
    Retrieves a Splitgill client object. If Splitgill is not configured yet on the VDS
    plugin, an exception is raised.

    :returns: an SplitgillClient object
    """
    vds_plugin = get_plugin('versioned_datastore')
    if not vds_plugin.is_sg_configured:
        raise Exception('VDS plugin not configured yet')
    return vds_plugin.sg_client


def es_client() -> Elasticsearch:
    """
    Retrieves an Elasticsearch client for use on the in use cluster. If Splitgill is not
    configured yet on the VDS plugin, an exception is raised.

    :returns: an Elasticsearch object
    """
    vds_plugin = get_plugin('versioned_datastore')
    if not vds_plugin.is_sg_configured:
        raise Exception('VDS plugin not configured yet')
    return vds_plugin.elasticsearch_client


def mongo_client() -> MongoClient:
    """
    Retrieves a Mongo client for use on the in use database instance. If Splitgill is
    not configured yet on the VDS plugin, an exception is raised.

    :returns: an MongoClient object
    """
    vds_plugin = get_plugin('versioned_datastore')
    if not vds_plugin.is_sg_configured:
        raise Exception('VDS plugin not configured yet')
    return vds_plugin.mongo_client


def get_latest_version(resource_id) -> Optional[int]:
    """
    Retrieves the latest version of the given resource from the status index.

    :param resource_id: the resource's id
    :returns: the version or None if the resource isn't indexed
    """
    return get_database(resource_id).get_elasticsearch_version()


def get_sg_name(resource_id: str) -> str:
    """
    Adds the configured prefix to the start of the resource id to get the index name for
    the resource data in elasticsearch.

    :param resource_id: the resource id
    :returns: the resource's Splitgill database name
    """
    prefix = toolkit.config.get('ckanext.versioned_datastore.sg_prefix', '')
    return f'{prefix}{resource_id}'


def unprefix_sg_name(sg_name: str) -> str:
    """
    Removes the configured prefix from the start of the index name to get the resource
    id.

    :param sg_name: the Spitgill database name
    :returns: the resource's id
    """
    prefix = toolkit.config.get('ckanext.versioned_datastore.sg_prefix', '')
    return sg_name[len(prefix) :]


def unprefix_index_name(sg_index_name: str) -> str:
    """
    Extracts the resource ID from the given Splitgill index name by removing the
    Splitgill specific parts, plus removing the prefix (if one is configured). If the
    resource ID cannot be extracted, a ValueError is raised.

    :param sg_index_name: the Splitgill index name
    :returns: the resource's ID
    """
    # all indexes have data- at the start and -latest or -arc-# on the end
    regexes = [re.compile(r'data-(.*)-latest'), re.compile(r'data-(.*)-arc-[0-9]+')]
    for regex in regexes:
        match = regex.match(sg_index_name)
        if match:
            return unprefix_sg_name(match.group(1))
    raise ValueError(f'Failed to extract resource name from index: {sg_index_name}')


class ReadOnlyResourceException(toolkit.ValidationError):
    """
    Raised when a write operation of some variety is attempted on a resource which has
    been marked as read only.
    """

    pass


class RawResourceException(toolkit.ValidationError):
    """
    Raised when trying to ingest a resource that has been marked with "disable_parsing".
    """

    pass


def is_resource_read_only(resource_id: str) -> bool:
    """
    Loops through the plugin implementations checking if any of them want the given
    resource id to be read only.

    :returns: True if the resource should be treated as read only, False if not
    """
    return any(
        plugin.vds_is_read_only_resource(resource_id)
        for plugin in ivds_implementations()
    )


def is_datastore_resource(resource_id: str) -> bool:
    """
    Checks if any data has made it to Elasticsearch for this resource ID. Note that this
    only checks Elasticsearch, it doesn't check MongoDB, and is therefore intended to
    simply test if there is any searchable data for the resource.

    :param resource_id: the resource id
    :returns: True if the resource is a datastore resource, False if not
    """
    return get_database(resource_id).get_elasticsearch_version() is not None


def is_datastore_only_resource(resource_url: str) -> bool:
    """
    Checks whether the resource url is a datastore only resource url. When uploading
    data directly to the API without using a source file/URL the url of the resource
    will be set to "_datastore_only_resource" to indicate that as such. This function
    checks to see if the resource URL provided is one of these URLs. Note that we check
    a few different scenarios as CKAN has the nasty habit of adding a protocol onto the
    front of these URLs when saving the resource, sometimes.

    :param resource_url: the URL of the resource
    :returns: True if the resource is a datastore only resource, False if not
    """
    return (
        resource_url == common.DATASTORE_ONLY_RESOURCE
        or resource_url == f'http://{common.DATASTORE_ONLY_RESOURCE}'
        or resource_url == f'https://{common.DATASTORE_ONLY_RESOURCE}'
    )


def is_ingestible(resource: dict) -> bool:
    """
    Returns True if the resource can be ingested into the datastore and False if not. To
    be ingestible, the resource must either be a datastore only resource (signified by
    the url being set to _datastore_only_resource) or have a format that we can ingest
    (the format field on the resource is used for this, not the URL). If the url is
    None, False is returned. This is technically not possible due to a Resource model
    constraint, but it's worth covering off anyway.

    :param resource: the resource dict
    :returns: True if it is, False if not
    """
    if resource.get('url', None) is None:
        return False

    resource_format = resource.get('format', None)
    not_raw = not resource.get('disable_parsing', False)
    return (
        not_raw
        and is_datastore_only_resource(resource['url'])
        or (
            resource_format is not None
            and resource_format.lower() in common.ALL_FORMATS
        )
    )


T = TypeVar('T', bound=IVersionedDatastore)
U = TypeVar('U', bound=IVersionedDatastoreDownloads)
V = TypeVar('V', bound=IVersionedDatastoreQuerySchema)


def ivds_implementations() -> Iterable[T]:
    yield from plugins.PluginImplementations(IVersionedDatastore)


def idownload_implementations() -> Iterable[U]:
    yield from plugins.PluginImplementations(IVersionedDatastoreDownloads)


def iqs_implementations() -> Iterable[V]:
    yield from plugins.PluginImplementations(IVersionedDatastoreQuerySchema)


def clear_cached_metadata():
    """
    Clears cached package and resource metadata, e.g. lists of public resource IDs.

    Cached functions have to be added manually and cleared individually. This is partly
    so we can control exactly which functions to clear, and partly because beaker does
    not seem to have a way to clear an entire region.
    :return:
    """
    cache_opts = cache_regions.get('vds')
    if cache_opts is None:
        # this shouldn't happen, but just in case
        cache_opts = {}
        for k, v in toolkit.config.items():
            if k.startswith('ckanext.versioned_datastore.cache.'):
                cache_opts[k.split('.')[-1]] = v
    # cache_managers does not usually seem to be populated so just construct a new ref
    cache_manager = CacheManager(**cache_opts)
    # manually list the cached functions to be cleared
    cached_functions = [get_public_resources, get_latest_resource_fields]
    for func in cached_functions:
        # each function has its own namespace that needs to be cleared
        try:
            cache = cache_manager.get_cache(func._arg_namespace)
            cache.clear()
            log.info(f'Cleared cache for {func.__name__}')
        except Exception as e:
            log.error(f'Failed to clear cache for {func.__name__}: {e}')
