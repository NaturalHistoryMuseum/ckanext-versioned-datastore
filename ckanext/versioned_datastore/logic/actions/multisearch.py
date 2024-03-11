from collections import defaultdict
from datetime import datetime
from typing import Dict

import jsonschema
from ckantools.decorators import action
from ckantools.timer import Timer
from elasticsearch_dsl import MultiSearch, A
from splitgill.utils import to_timestamp

from ckan.plugins import toolkit, PluginImplementations, plugin_loaded
from .meta import help, schema
from ...interfaces import IVersionedDatastore
from ...lib import common
from ...lib.basic_query.utils import convert_to_multisearch
from ...lib.datastore_utils import (
    prefix_resource,
    unprefix_index,
    iter_data_fields,
    trim_index_name,
    prefix_field,
)
from ...lib.query.fields import (
    get_all_fields,
    select_fields,
    get_single_resource_fields,
    get_mappings,
)
from ...lib.query.query_log import log_query
from ...lib.query.schema import (
    get_latest_query_version,
    InvalidQuerySchemaVersionError,
    validate_query,
    translate_query,
    hash_query,
    normalise_query,
)
from ...lib.query.slugs import create_slug, resolve_slug, create_nav_slug
from ...lib.query.utils import (
    get_available_datastore_resources,
    determine_resources_to_search,
    determine_version_filter,
    calculate_after,
    find_searched_resources,
)


@action(
    schema.datastore_multisearch(), help.datastore_multisearch, toolkit.side_effect_free
)
def datastore_multisearch(
    context,
    query=None,
    query_version=None,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
    size=100,
    after=None,
    top_resources=False,
    timings=False,
):
    """
    Performs a search across multiple resources at the same time and returns the results
    in descending _id and index name order (the index name is included to ensure unique
    sorting otherwise the after value based pagination won't work properly).

    :param context: the context dict from the action call
    :param query: the query dict. If None (default) then an empty query is used
    :param query_version: the version of the query schema the query is using. If None (default) then
                          the latest query schema version is used
    :param version: the version to search the data at. If None (default) the current time is used
    :param resource_ids: the list of resource to search. If None (default) then all the resources
                         the user has access to are queried. If a list of resources are passed then
                         any resources not accessible to the user will be removed before querying
    :param resource_ids_and_versions: a dict of resources and versions to search each of them at.
                                      This allows precise searching of each resource at a specific
                                      parameter. If None (default) then the resource_ids parameter
                                      is used together with the version parameter. If this parameter
                                      is provided though, it takes priority over the resource_ids
                                      and version parameters.
    :param size: the number of records to return. Defaults to 100 if not provided and must be
                 between 0 and 1000.
    :param after: pagination after value that has come from a previous result. If None (default)
                  this parameter is ignored.
    :param top_resources: whether to include information about the resources with the most results
                          in them (defaults to False) in the result
    :param timings: whether to include timing information in the result dict
    :return: a dict of results including the records and total
    """
    # provide some more complex defaults for some parameters if necessary
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()
    size = max(0, min(size, 1000))

    timer = Timer()

    query = normalise_query(query, query_version)

    try:
        # validate and translate the query into an elasticsearch-dsl Search object
        validate_query(query, query_version)
        timer.add_event('validate')
        search = translate_query(query, query_version)
        timer.add_event('translate')
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    # figure out which resources we're searching
    resource_ids, skipped_resource_ids = determine_resources_to_search(
        context, resource_ids, resource_ids_and_versions
    )
    timer.add_event('determine_resources')
    if not resource_ids:
        raise toolkit.ValidationError(
            "The requested resources aren't accessible to this user"
        )

    # add the version filter necessary given the parameters and the resources we're searching
    version_filter = determine_version_filter(
        version, resource_ids, resource_ids_and_versions
    )
    search = search.filter(version_filter)
    timer.add_event('version_filter')

    # add a simple default sort to ensure we get an after value for pagination. We use a combination
    # of the modified date, id of the record and the index it's in so that we get a unique sort
    search = search.sort(
        # not all indexes have a modified field so we need to provide the unmapped_type option
        {'meta.version': 'desc'},
        {'data._id': 'desc'},
        {'_index': 'desc'},
    )
    # add the after if there is one
    if after is not None:
        search = search.extra(search_after=after)
    # add the size parameter. We pass the requested size + 1 to allow us to determine if the results
    # we find represent the last page of results or not
    search = search.extra(size=size + 1)
    # add the resource indexes we're searching on
    search = search.index(
        [prefix_resource(resource_id) for resource_id in resource_ids]
    )

    if top_resources:
        # gather the number of hits in the top 10 most frequently represented indexes if requested
        search.aggs.bucket('indexes', 'terms', field='_index')

    # create a multisearch for this one query - this ensures there aren't any issues with the length
    # of the URL as the index list is passed as a part of the body
    multisearch = MultiSearch(using=common.ES_CLIENT).add(search)
    timer.add_event('search_params')

    # run the search and get the only result from the search results list
    result = next(iter(multisearch.execute()))
    timer.add_event('run')

    hits, next_after = calculate_after(result, size)

    response = {
        'total': result.hits.total,
        'after': next_after,
        'records': [
            {
                'data': hit.data.to_dict(),
                # should we provide the name too? If so cache a map of id -> name, then update it if we
                # don't find the id in the map
                'resource': trim_index_name(hit.meta.index),
            }
            for hit in hits
        ],
        'skipped_resources': skipped_resource_ids,
    }

    if top_resources:
        # include the top resources if requested
        response['top_resources'] = [
            {trim_index_name(bucket['key']): bucket['doc_count']}
            for bucket in result.aggs.to_dict()['indexes']['buckets']
        ]
    timer.add_event('response')

    # allow plugins to modify the fields object
    for plugin in PluginImplementations(IVersionedDatastore):
        response = plugin.datastore_multisearch_modify_response(response)
    timer.add_event('response_modifiers')

    log_query(query, 'multisearch')
    timer.add_event('log')

    if timings:
        response['timings'] = timer.to_dict()
    return response


@action(schema.datastore_create_slug(), help.datastore_create_slug)
def datastore_create_slug(
    context,
    query=None,
    query_version=None,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
    pretty_slug=True,
    nav_slug=False,
):
    """
    Creates a slug for the given multisearch parameters and returns it. This slug can be
    used, along with the resolve_slug action, to retrieve, at any point after the slug
    is created, the query parameters passed to this action. The slug is unique for the
    given query parameters and passing the same query parameters again at a later point
    will result in the same slug being returned.

    :param context: the context dict from the action call
    :param query: the query dict. If None (default) then an empty query is used
    :param query_version: the version of the query schema the query is using. If None (default) then
                          the latest query schema version is used
    :param version: the version to search the data at. If None (default) the current time is used
    :param resource_ids: the list of resource to search. If None (default) then all the resources
                         the user has access to are queried. If a list of resources are passed then
                         any resources not accessible to the user will be removed before querying
    :param resource_ids_and_versions: a dict of resources and versions to search each of them at.
                                      This allows precise searching of each resource at a specific
                                      parameter. If None (default) then the resource_ids parameter
                                      is used together with the version parameter. If this parameter
                                      is provided though, it takes priority over the resource_ids
                                      and version parameters.
    :param pretty_slug: whether to produce a "pretty" slug or not. If True (the default) a selection
                        of 2 adjectives and an animal will be used to create the slug, otherwise if
                        False, a uuid will be used
    :param nav_slug: if this is True, a temporary navigational slug will be produced
                     instead of a standard slug
    :return: a dict containing the slug and whether it was created during this function call or not
    """
    if query is None:
        query = {}
    if query_version and query_version.lower().startswith('v0'):
        # this is an old/basic query so we need to convert it first
        query = convert_to_multisearch(query)
        query_version = None
    if query_version is None:
        query_version = get_latest_query_version()

    try:
        if nav_slug:
            is_new, slug = create_nav_slug(
                context, query, version, resource_ids, resource_ids_and_versions
            )
        else:
            is_new, slug = create_slug(
                context,
                query,
                query_version,
                version,
                resource_ids,
                resource_ids_and_versions,
                pretty_slug=pretty_slug,
            )
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    if slug is None:
        raise toolkit.ValidationError('Failed to generate new slug')

    return {
        'slug': slug.get_slug_string(),
        'is_new': is_new,
        'is_reserved': False
        if nav_slug
        else slug.reserved_pretty_slug == slug.get_slug_string(),
    }


@action(
    schema.datastore_resolve_slug(),
    help.datastore_resolve_slug,
    toolkit.side_effect_free,
)
def datastore_resolve_slug(slug):
    """
    Resolves the given slug and returns the query parameters used to create it.

    :param slug: the slug
    :return: the query parameters and the creation time in a dict
    """
    # try resolving the slug first
    resolved = resolve_slug(slug)
    if resolved:
        result = {
            k: getattr(resolved, k)
            for k in (
                'query',
                'query_version',
                'version',
                'resource_ids',
                'resource_ids_and_versions',
            )
        }
        result['created'] = resolved.created.isoformat()
        if result.get('query_version') == 'v0':
            result['query'] = convert_to_multisearch(result['query'])
            result['query_version'] = get_latest_query_version()
        return result

    # then check if it's a query DOI
    if plugin_loaded('query_dois'):
        from ckanext.query_dois.model import QueryDOI
        from ckan import model

        resolved = model.Session.query(QueryDOI).filter(QueryDOI.doi == slug).first()
        if resolved:
            if resolved.query_version == 'v0':
                query = convert_to_multisearch(resolved.query)
                query_version = get_latest_query_version()
            else:
                query = resolved.query
                query_version = resolved.query_version
            return {
                'query': query,
                'query_version': query_version,
                'version': resolved.requested_version,
                'resource_ids': list(resolved.resources_and_versions.keys()),
                'resource_ids_and_versions': resolved.resources_and_versions,
                'created': resolved.timestamp.isoformat(),
            }

    # if both slug and DOI have failed
    raise toolkit.ValidationError('Slug not found')


@action(
    schema.datastore_field_autocomplete(),
    help.datastore_field_autocomplete,
    toolkit.side_effect_free,
)
def datastore_field_autocomplete(context, text='', resource_ids=None, lowercase=False):
    """
    Given a text value, finds fields that contain the given text from the given resource
    (or all resource if no resources are passed).

    :param context: the context dict from the action call
    :param text: the text to search with (default is an empty string)
    :param resource_ids: a list of resources to find fields from, if None (the default) all resource
                         fields are searched
    :param lowercase: whether to do a lowercase check or not, essentially whether to be case
                      insensitive. Default: True, be case insensitive.
    :return: the fields and the resources they came from as a dict
    """
    # figure out which resources should be searched
    resource_ids = get_available_datastore_resources(context, resource_ids)
    if not resource_ids:
        raise toolkit.ValidationError(
            "The requested resources aren't accessible to this user"
        )

    mappings = get_mappings(resource_ids)

    fields = defaultdict(dict)

    for index, mapping in mappings.items():
        resource_id = unprefix_index(index)

        for field_path, config in iter_data_fields(mapping):
            if any(
                text in (part.lower() if lowercase else part) for part in field_path
            ):
                fields['.'.join(field_path)][resource_id] = {
                    'type': config['type'],
                    'fields': {
                        f: c['type'] for f, c in config.get('fields', {}).items()
                    },
                }

    return {
        'count': len(fields),
        'fields': fields,
    }


@action(
    schema.datastore_guess_fields(),
    help.datastore_guess_fields,
    toolkit.side_effect_free,
)
def datastore_guess_fields(
    context,
    query=None,
    query_version=None,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
    size=10,
    ignore_groups=None,
):
    """
    Guesses the fields that are most relevant to show with the given query.

    If only one resource is included in the search then the requested number of fields from the
    resource at the required version are returned in ingest order if the details are available.

    If multiple resources are queried, the most common fields across the resource under search are
    returned. The fields are grouped together in an attempt to match the same field name in
    different cases across different resources. The most common {size} groups are returned.

    The groups returned are ordered firstly by the number of resources they appear in in descending
    order, then if there are ties, the number of records the group finds is used and this again is
    ordered in a descending fashion.

    :param context: the context dict from the action call
    :param query: the query
    :param query_version: the query schema version
    :param version: the version to search at
    :param resource_ids: the resource ids to search in
    :param resource_ids_and_versions: a dict of resource ids -> versions to search at
    :param size: the number of groups to return
    :param ignore_groups: a list of groups to ignore from the results (default: None)
    :return: a list of groups
    """
    # provide some more complex defaults for some parameters if necessary
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()
    ignore_groups = (
        set(g.lower() for g in ignore_groups) if ignore_groups is not None else set()
    )

    query = normalise_query(query, query_version)

    try:
        # validate and translate the query into an elasticsearch-dsl Search object
        validate_query(query, query_version)
        search = translate_query(query, query_version)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    # figure out which resources we're searching
    resource_ids, skipped_resource_ids = determine_resources_to_search(
        context, resource_ids, resource_ids_and_versions
    )
    if not resource_ids:
        raise toolkit.ValidationError(
            "The requested resources aren't accessible to this user"
        )

    if version is None:
        version = to_timestamp(datetime.now())
    # add the version filter necessary given the parameters and the resources we're searching
    version_filter = determine_version_filter(
        version, resource_ids, resource_ids_and_versions
    )
    search = search.filter(version_filter)

    # add the size parameter, we don't want any records back
    search = search.extra(size=0)

    resource_ids = find_searched_resources(search, resource_ids)

    all_fields = get_all_fields(resource_ids)
    for group in ignore_groups:
        all_fields.ignore(group)

    # allow plugins to modify the fields object
    for plugin in PluginImplementations(IVersionedDatastore):
        all_fields = plugin.datastore_modify_guess_fields(resource_ids, all_fields)

    if len(resource_ids) == 1:
        resource_id = resource_ids[0]
        if resource_ids_and_versions is None:
            up_to_version = version
        else:
            up_to_version = resource_ids_and_versions[resource_id]
        return get_single_resource_fields(
            all_fields, resource_id, up_to_version, search, size
        )
    else:
        size = max(0, min(size, 25))
        return select_fields(all_fields, search, size)


@action(
    schema.datastore_value_autocomplete(),
    help.datastore_value_autocomplete,
    toolkit.side_effect_free,
)
def datastore_value_autocomplete(
    context,
    field,
    prefix,
    query=None,
    query_version=None,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
    size=20,
    after=None,
):
    """
    Returns a list of values in alphabetical order from the given field that start with
    the given prefix. The values have to be from the provided resource ids and be from
    documents that match the given query. The after parameter can be used to get the
    next set of values providing pagination. The resulting list is limited to a maximum
    size of 20 values.

    :param context: the context dict from the action call
    :param field: the field to get the values from
    :param prefix: the prefix value to search with (this can be missing/blank to just return the
                   first values)
    :param query: the query
    :param query_version: the query schema version
    :param version: the version to search at
    :param resource_ids: the resource ids to search in
    :param resource_ids_and_versions: a dict of resource ids -> versions to search at
    :param size: the number of values to return (max 500)
    :param after: the after value to use (provides pagination)
    :return: a list of values that match the given prefix
    """
    # provide some more complex defaults for some parameters if necessary
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()
    # limit the size so that it is between 1 and 500
    size = max(1, min(size, 500))

    query = normalise_query(query, query_version)

    try:
        # validate and translate the query into an elasticsearch-dsl Search object
        validate_query(query, query_version)
        search = translate_query(query, query_version)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    # figure out which resources we're searching
    resource_ids, skipped_resource_ids = determine_resources_to_search(
        context, resource_ids, resource_ids_and_versions
    )
    if not resource_ids:
        raise toolkit.ValidationError(
            "The requested resources aren't accessible to this user"
        )

    # add the version filter necessary given the parameters and the resources we're searching
    version_filter = determine_version_filter(
        version, resource_ids, resource_ids_and_versions
    )
    search = search.filter(version_filter)

    # only add the prefix filter to the search if one is provided
    if prefix:
        search = search.filter('prefix', **{prefix_field(field): prefix})

    # we don't need any results so set the size to 0
    search = search.extra(size=0)

    # modify the search so that it has the aggregation required to get the autocompletion results
    search.aggs.bucket(
        'field_values',
        'composite',
        size=size,
        sources={field: A('terms', field=prefix_field(field), order='asc')},
    )
    # if there's an after included, add it into the aggregation
    if after:
        search.aggs['field_values'].after = {field: after}

    # add the resource indexes we're searching on
    search = search.index(
        [prefix_resource(resource_id) for resource_id in resource_ids]
    )

    # create a multisearch for this one query - this ensures there aren't any issues with the length
    # of the URL as the index list is passed as a part of the body
    multisearch = MultiSearch(using=common.ES_CLIENT).add(search)

    # run the search and get the only result from the search results list
    result = next(iter(multisearch.execute()))

    # get the results we're interested in
    agg_result = result.aggs.to_dict()['field_values']
    # return a dict of results, but only include the after details if there are any to include
    response = {
        'values': [bucket['key'][field] for bucket in agg_result['buckets']],
    }
    if 'after_key' in agg_result:
        response['after'] = agg_result['after_key'][field]
    if skipped_resource_ids:
        result['skipped_resources'] = skipped_resource_ids
    return response


@action(
    schema.datastore_hash_query(), help.datastore_hash_query, toolkit.side_effect_free
)
def datastore_hash_query(query=None, query_version=None):
    """
    Hashes the given query at the given query schema and returns the hex digest.

    :param query: the query dict
    :param query_version: the query version
    :return: the hex digest of the query
    """
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()

    query = normalise_query(query, query_version)

    try:
        validate_query(query, query_version)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    return hash_query(query, query_version)


@action(
    schema.datastore_edit_slug(), help.datastore_edit_slug, toolkit.side_effect_free
)
def datastore_edit_slug(context, current_slug, new_reserved_slug):
    slug = resolve_slug(current_slug)
    if slug is None:
        raise toolkit.Invalid(f'The slug {current_slug} does not exist')
    if slug.reserved_pretty_slug and not context['auth_user_obj'].sysadmin:
        raise toolkit.NotAuthorized(
            'Only sysadmins can replace existing reserved slugs.'
        )
    slug.reserved_pretty_slug = new_reserved_slug.lower()
    slug.commit()
    return slug.as_dict()


@action(
    schema.datastore_multisearch_counts(),
    help.datastore_multisearch_counts,
    toolkit.side_effect_free,
)
def datastore_multisearch_counts(
    context,
    query=None,
    query_version=None,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
) -> Dict[str, int]:
    """
    Efficiently counts the number of records in each of the given resources matching the
    given query. A dict of resource IDs -> count is returned. If no records in a
    resource match the query then it will appear in the dict with a count value of 0.

    :param context: the context dict from the action call
    :param query: the query dict. If None (default) then an empty query is used
    :param query_version: the version of the query schema the query is using. If None
                          (default) then the latest query schema version is used
    :param version: the version to search the data at. If None (default) the current
                    time is used
    :param resource_ids: the list of resource to search. If None (default) then all the
                         resources the user has access to are queried. If a list of
                         resources are passed then any resources not accessible to the
                         user will be removed before querying
    :param resource_ids_and_versions: a dict of resources and versions to search each of
                                      them at. This allows precise searching of each
                                      resource at a specific parameter. If None
                                      (default) then the resource_ids parameter is used
                                      together with the version parameter. If this
                                      parameter is provided though, it takes priority
                                      over the resource_ids and version parameters.
    :return: a dict of resource IDs -> count
    """
    # provide some more complex defaults for some parameters if necessary
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()

    query = normalise_query(query, query_version)

    try:
        # validate and translate the query into an elasticsearch-dsl Search object
        validate_query(query, query_version)
        search = translate_query(query, query_version)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    # figure out which resources we're searching
    resource_ids, skipped_resource_ids = determine_resources_to_search(
        context, resource_ids, resource_ids_and_versions
    )
    if not resource_ids:
        raise toolkit.ValidationError(
            "The requested resources aren't accessible to this user"
        )

    # add the version filter necessary given the parameters and the resources we're
    # searching
    version_filter = determine_version_filter(
        version, resource_ids, resource_ids_and_versions
    )
    search = search.filter(version_filter)

    # add the resource indexes we're searching on
    search = search.index(
        [prefix_resource(resource_id) for resource_id in resource_ids]
    )
    # no results please, we aren't going to use them
    search = search.extra(size=0)
    # use an aggregation to get the hit count of each resource, set the size to the
    # number of resources we're querying to ensure we get all counts in one go and don't
    # have to paginate with a composite agg
    search.aggs.bucket("counts", "terms", field="_index", size=len(resource_ids))

    # create a multisearch for this one query - this ensures there aren't any issues
    # with the length of the URL as the index list is passed as a part of the body
    multisearch = MultiSearch(using=common.ES_CLIENT).add(search)

    # run the search and get the only result from the search results list
    result = next(iter(multisearch.execute()))

    # build the response JSON
    counts = {
        trim_index_name(bucket["key"]): bucket["doc_count"]
        for bucket in result.aggs.to_dict()["counts"]["buckets"]
    }
    # add resources that didn't have any hits into the counts dict too
    counts.update(
        {resource_id: 0 for resource_id in resource_ids if resource_id not in counts}
    )
    return counts
