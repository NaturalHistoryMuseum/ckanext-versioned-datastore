from copy import copy
from datetime import datetime

from elasticsearch_dsl import Search, MultiSearch
from splitgill.search import create_version_query, create_index_specific_version_filter
from splitgill.utils import to_timestamp

from ckan import model
from ckan.plugins import toolkit
from .. import common
from ..datastore_utils import prefix_resource, get_last_after, trim_index_name


def get_available_datastore_resources(context, only=None):
    """
    Returns a set of resource ids accessible to the current user based on the given
    context that are also datastore resources. If the only parameter is passed then it
    is used to filter the set of resources that are returned to include only ones in the
    only list. If the parameter is not passed, or indeed is falsey in any way (such as
    an empty list) then all resource ids available to the user are returned.

    :param context: the dict ckan context to request auth against
    :param only: optional list of resource ids to filter the returned list by. Defaults to None
                 which indicates all available resources should be returned
    :return: a set of resource ids
    """
    # retrieve all resource ids and associated package ids direct from the database for speed
    query = (
        model.Session.query(model.Resource)
        .join(model.Package)
        .filter(model.Resource.state == 'active')
        .filter(model.Package.state == 'active')
        .with_entities(model.Resource.id, model.Package.id)
    )
    # retrieve the names in the status index
    status_search = Search(
        index=common.CONFIG.elasticsearch_status_index_name, using=common.ES_CLIENT
    ).source(['name'])

    if only:
        # apply filters to only get the resources passed in the only list
        query = query.filter(model.Resource.id.in_(only))
        status_search = status_search.filter('terms', name=only)

    # complete the database query and the elasticsearch query
    resources_and_packages = list(query)
    datastore_resources = {hit.name for hit in status_search.scan()}

    # this is the set of resource ids we will populate and return
    resource_ids = set()

    # when a resource is checked in ckan to see if can be accessed by the current user, it's package
    # is inspected. To avoid inspecting the same package over and over again when multiple resources
    # exist under a package, we'll cache the results
    package_access_cache = {}

    for resource_id, package_id in resources_and_packages:
        # if the resource isn't a datastore resource or we've already handled it, ignore it
        if resource_id not in datastore_resources or resource_id in resource_ids:
            continue

        # check the cache, we'll get back either True, False or None
        has_access = package_access_cache.get(package_id, None)

        if has_access:
            # access allowed, add to the list
            resource_ids.add(resource_id)
        elif has_access is None:
            # if the result of looking in the cache is None then there is no value for this package
            # in the cache and we need to do the work
            try:
                try:
                    # remove any cached package from the context so that we know we're
                    # definitely getting the permissions for *this* package
                    del context['package']
                except KeyError:
                    pass
                toolkit.check_access('package_show', context, {'id': package_id})
                package_access_cache[package_id] = True
                # access allowed, add to the list
                resource_ids.add(resource_id)
            except toolkit.NotAuthorized:
                package_access_cache[package_id] = False
        else:
            # skip, there is no access
            continue

    return resource_ids


def determine_resources_to_search(
    context, resource_ids=None, resource_ids_and_versions=None
):
    """
    Determines the resources to search from the given parameters. The set of resource
    ids returned contains only the resources that the user has access to (this is
    determined using the context) and are datastore active. If resource ids are provided
    through either the resource_ids or resource_ids_and_versions parameters then only
    these resource ids will be returned, if indeed they are accessible to the user.

    :param context: the context dict allowing us to determine the user and do auth on the resources
    :param resource_ids: a list of resources to search
    :param resource_ids_and_versions: a dict of resources and versions to search at
    :return: 2-tuple containing a list of resource ids to search and a list of resource ids that
             have been skipped because the user doesn't have access to them or they aren't datastore
             resources
    """
    # validate the resource_ids passed in. If the resource_ids_and_versions parameter is in use then
    # it is taken as the resource_ids source and resource_ids is ignored
    if resource_ids_and_versions:
        requested_resource_ids = list(resource_ids_and_versions.keys())
    else:
        requested_resource_ids = resource_ids
    # this will return the subset of the requested resource ids that the user can search over
    resource_ids = get_available_datastore_resources(context, requested_resource_ids)

    if requested_resource_ids is not None:
        skipped_resources = [
            rid for rid in requested_resource_ids if rid not in resource_ids
        ]
    else:
        skipped_resources = []
    return list(resource_ids), skipped_resources


def determine_version_filter(
    version=None, resource_ids=None, resource_ids_and_versions=None
):
    """
    Determine and return the elasticsearch-dsl filter which can filter on the version
    extracted from the given parameters.

    :param version: the version to filter on across all resources
    :param resource_ids: the resource to search
    :param resource_ids_and_versions: a dict of resource ids -> versions providing resource specific
                                      versions for search
    :return: an elasticsearch-dsl object
    """
    if not resource_ids_and_versions:
        # default the version to now if necessary
        if version is None:
            version = to_timestamp(datetime.now())
        # just use a single version filter if we don't have any resource specific versions
        return create_version_query(version)
    else:
        # run through the resource specific versions provided and ensure they're rounded down
        indexes_and_versions = {}
        for resource_id in resource_ids:
            target_version = resource_ids_and_versions[resource_id]
            if target_version is None:
                raise toolkit.ValidationError(
                    f'Valid version not given for {resource_id}'
                )
            index = prefix_resource(resource_id)
            rounded_version = common.SEARCH_HELPER.get_rounded_versions(
                [index], target_version
            )[index]
            indexes_and_versions[index] = rounded_version

        return create_index_specific_version_filter(indexes_and_versions)


def calculate_after(result, size):
    """
    Calculate the after value for the given search result. It is assumed that the size
    used when the search was completed is 1 larger than the size passed as a parameter
    to this function.

    :param result: the elasticsearch result object
    :param size: the number of results
    :return: a 2-tuple containing the list of hits and the next after value
    """
    if len(result.hits) > size:
        # there are more results, trim off the last hit as it wasn't requested
        hits = result.hits[:-1]
        next_after = get_last_after(hits)
    else:
        # there are no more results beyond the ones we're going to pass back
        next_after = None
        hits = result.hits
    return hits, next_after


def chunk_iterator(iterable, chunk_size):
    """
    Iterates over an iterable, yielding lists of size chunk_size until the iterable is
    exhausted. The final list could be smaller than chunk_size but will always have a
    length > 0.

    :param iterable: the iterable to chunk up
    :param chunk_size: the maximum size of each yielded chunk
    :return: a generator of list chunks
    """
    chunk = []
    for element in iterable:
        chunk.append(element)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def find_searched_resources(search, resource_ids):
    """
    Given a search and a list of resource ids to search in, returns a list of the
    resources that are actually included in the search results.

    :param search: an elasticsearch-dsl object
    :param resource_ids: a list of resource ids
    :return: a list of resource ids
    """
    # we have to make a copy as aggs don't return a clone :(
    search_copy = copy(search)
    search_copy = search_copy.index(
        [prefix_resource(resource_id) for resource_id in resource_ids]
    )
    search_copy.aggs.bucket('indexes', 'terms', field='_index')
    multisearch = MultiSearch(using=common.ES_CLIENT).add(search_copy)
    result = next(iter(multisearch.execute()))
    return [
        trim_index_name(bucket['key'])
        for bucket in result.aggs.to_dict()['indexes']['buckets']
        if bucket['doc_count'] > 0
    ]


def get_resources_and_versions(
    resource_ids=None,
    resource_ids_and_versions=None,
    version=None,
    allow_non_datastore=False,
):
    """
    Get a list of resource ids and a dict of resource ids and versions from either, e.g.
    get the list of resource ids from a resource id and version dict.

    :param resource_ids: a list of resource ids
    :param resource_ids_and_versions: a dict of resource id: resource version
    :param version: a datestamp used as a default version for resources without a version
    :param allow_non_datastore: allow non datastore resources to be included (will be
                                returned with common.NON_DATASTORE_VERSION)
    :return: a tuple of resource_ids, resource_ids_and_versions
    """

    if resource_ids_and_versions is None:
        resource_ids_and_versions = {}
    else:
        # use the resource_ids_and_versions dict first over the resource_ids and version params
        resource_ids = list(resource_ids_and_versions.keys())

    # first see what's available from the datastore
    available_resource_ids = list(get_available_datastore_resources({}, resource_ids))
    if (not available_resource_ids) and (not allow_non_datastore):
        raise toolkit.ValidationError(
            "The requested resources aren't accessible to this user"
        )

    unavailable_resource_ids = [
        rid for rid in resource_ids or [] if rid not in available_resource_ids
    ]
    non_datastore_resources = []

    if allow_non_datastore:
        resource_show = toolkit.get_action('resource_show')
        for resource_id in unavailable_resource_ids:
            resource = resource_show({}, {'id': resource_id})
            # if we get nothing back there's probably an access issue; if it's a
            # datastore resource something went wrong earlier
            if resource and not resource['datastore_active']:
                available_resource_ids.append(resource_id)
                non_datastore_resources.append(resource_id)

    rounded_resource_ids_and_versions = {}
    # see if a version was provided; we'll use this if a resource id we're searching doesn't
    # have a directly assigned version (i.e. it was absent from the resource_ids_and_versions
    # dict, or that parameter wasn't provided)
    if version is None:
        version = to_timestamp(datetime.now())
    for resource_id in available_resource_ids:
        if resource_id in non_datastore_resources:
            rounded_resource_ids_and_versions[
                resource_id
            ] = common.NON_DATASTORE_VERSION
            continue
        # try to get the target version from the passed resource_ids_and_versions dict, but if
        # it's not in there, default to the version variable
        target_version = resource_ids_and_versions.get(resource_id, version)
        index = prefix_resource(resource_id)
        # round the version down to ensure we search the exact version requested
        rounded_version = common.SEARCH_HELPER.get_rounded_versions(
            [index], target_version
        )[index]
        if rounded_version is not None:
            # resource ids without a rounded version are skipped
            rounded_resource_ids_and_versions[resource_id] = rounded_version

    return available_resource_ids, rounded_resource_ids_and_versions


def convert_small_or_groups(query):
    """
    Convert OR groups containing only 1 item to AND groups.

    :param query: a multisearch query dict
    :return: the query with a converted filter dict, if applicable
    """
    if 'filters' not in query:
        return query

    def _convert(*filters):
        items = []
        for term_or_group in filters:
            k, v = list(term_or_group.items())[0]
            if k not in ['and', 'or', 'not']:
                items.append(term_or_group)
            elif k != 'or' or len(v) != 1:
                # don't convert empty groups because those throw an error for all types
                items.append({k: _convert(*v)})
            else:
                items.append({'and': _convert(*v)})
        return items

    query['filters'] = _convert(query['filters'])[0]

    return query


def remove_empty_groups(query):
    """
    Remove empty groups from filter list.

    :param query: a multisearch query dict
    :return: the query with a processed filter dict, if applicable
    """
    if 'filters' not in query:
        return query

    def _convert(*filters):
        items = []
        for term_or_group in filters:
            k, v = list(term_or_group.items())[0]
            if k not in ['and', 'or', 'not']:
                items.append(term_or_group)
            elif len(v) > 0:
                items.append({k: _convert(*v)})
        return items

    processed_filters = _convert(query['filters'])
    if len(processed_filters) == 0:
        del query['filters']
    else:
        query['filters'] = processed_filters[0]

    return query
