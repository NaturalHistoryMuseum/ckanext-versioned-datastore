from collections import defaultdict

import jsonschema
from ckan.plugins import toolkit
from datetime import datetime
from eevee.search import create_version_query, create_index_specific_version_filter
from eevee.utils import to_timestamp
from elasticsearch_dsl import MultiSearch

from . import help
from .utils import action
from .. import schema
from ...lib import common
from ...lib.datastore_utils import prefix_resource, unprefix_index, iter_data_fields, \
    get_last_after, trim_index_name
from ...lib.query.schema import get_latest_query_version, InvalidQuerySchemaVersionError, \
    validate_query, translate_query
from ...lib.query.slugs import create_slug, resolve_slug
from ...lib.query.utils import get_available_datastore_resources


@action(schema.datastore_multisearch(), help.datastore_multisearch, toolkit.side_effect_free)
def datastore_multisearch(context, query=None, query_version=None, version=None, resource_ids=None,
                          resource_ids_and_versions=None, size=100, after=None,
                          top_resources=False):
    '''
    Performs a search across multiple resources at the same time and returns the results in
    descending _id and index name order (the index name is included to ensure unique sorting
    otherwise the after value based pagination won't work properly).

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
    :return: a dict of results including the records and total
    '''
    # provide some more complex defaults for some parameters if necessary
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()
    size = max(0, min(size, 1000))

    try:
        # validate and translate the query into an elasticsearch-dsl Search object
        validate_query(query, query_version)
        search = translate_query(query, query_version)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    # validate the resource_ids passed in. If the resource_ids_and_versions parameter is in use then
    # it is taken as the resource_ids source and resource_ids is ignored
    if resource_ids_and_versions:
        requested_resource_ids = list(resource_ids_and_versions.keys())
    else:
        requested_resource_ids = resource_ids
    # this will return the subset of the requested resource ids that the user can search over
    resource_ids = get_available_datastore_resources(context, requested_resource_ids)
    if not resource_ids:
        raise toolkit.ValidationError(u"The requested resources aren't accessible to this user")

    # add the appropriate version filter to the search
    if not resource_ids_and_versions:
        # default the version to now if necessary
        if version is None:
            version = to_timestamp(datetime.now())
        # just use a single version filter if we don't have any resource specific versions
        search = search.filter(create_version_query(version))
    else:
        # run through the resource specific versions provided and ensure they're rounded down
        indexes_and_versions = {}
        for resource_id in resource_ids:
            target_version = resource_ids_and_versions[resource_id]
            if target_version is None:
                raise toolkit.ValidationError(u"Valid version not given for {}".format(resource_id))
            index = prefix_resource(resource_id)
            rounded_version = common.SEARCH_HELPER.get_rounded_versions([index],
                                                                        target_version)[index]
            indexes_and_versions[index] = rounded_version

        search = search.filter(create_index_specific_version_filter(indexes_and_versions))

    # add a simple default sort to ensure we get an after value for pagination. We use a combination
    # of the id of the record and the index it's in so that we get a unique sort
    search = search.sort({u'data._id': u'desc'}, {u'_index': u'desc'})
    # add the after if there is one
    if after is not None:
        search = search.extra(search_after=after)
    # add the size parameter. We pass the requested size + 1 to allow us to determine if the results
    # we find represent the last page of results or not
    search = search.extra(size=size + 1)

    if top_resources:
        # gather the number of hits in the top 10 most frequently represented indexes if requested
        search.aggs.bucket(u'indexes', u'terms', field=u'_index')

    # create a multisearch for this one query - this ensures there aren't any issues with the length
    # of the URL as the index list is passed as a part of the body
    multisearch = MultiSearch(using=common.ES_CLIENT).add(search)
    # run the search and get the only result from the search results list
    result = next(iter(multisearch.execute()))

    # work out if there are any more results after this page of results or not
    if len(result.hits) > size:
        # there are more results, trim off the last hit as it wasn't requested
        hits = result.hits[:-1]
        next_after = get_last_after(hits)
    else:
        # there are no more results beyond the ones we're going to pass back
        next_after = None
        hits = result.hits

    response = {
        u'total': result.hits.total,
        u'after': next_after,
        u'records': [{
            u'data': hit.data.to_dict(),
            # should we provide the name too? If so cache a map of id -> name, then update it if we
            # don't find the id in the map
            u'resource': trim_index_name(hit.meta.index),
        } for hit in hits],
        # note that resource_ids is a set and therefore the in check is speedy
        u'skipped_resources': [rid for rid in requested_resource_ids if rid not in resource_ids],
    }

    if top_resources:
        # include the top resources if requested
        response[u'top_resources'] = [
            {trim_index_name(bucket[u'key']): bucket[u'doc_count']}
            for bucket in result.aggs.to_dict()[u'indexes'][u'buckets']
        ]

    return response


@action(schema.datastore_create_slug(), help.datastore_create_slug)
def datastore_create_slug(context, query=None, query_version=None, version=None, resource_ids=None,
                          resource_ids_and_versions=None, pretty_slug=True):
    '''
    Creates a slug for the given multisearch parameters and returns it. This slug can be used, along
    with the resolve_slug action, to retrieve, at any point after the slug is created, the query
    parameters passed to this action. The slug is unique for the given query parameters and passing
    the same query parameters again at a later point will result in the same slug being returned.

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
    :return: a dict containing the slug and whether it was created during this function call or not
    '''
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()

    try:
        is_new, slug = create_slug(context, query, query_version, version, resource_ids,
                                   resource_ids_and_versions, pretty_slug=pretty_slug)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    if slug is None:
        raise toolkit.ValidationError(u'Failed to generate new slug')

    return {
        u'slug': slug.get_slug_string(),
        u'is_new': is_new,
    }


@action(schema.datastore_resolve_slug(), help.datastore_resolve_slug, toolkit.side_effect_free)
def datastore_resolve_slug(slug):
    '''
    Resolves the given slug and returns the query parameters used to create it.

    :param slug: the slug
    :return: the query parameters and the creation time in a dict
    '''
    found_slug = resolve_slug(slug)
    if found_slug is None:
        raise toolkit.ValidationError(u'Slug not found')

    result = {k: getattr(found_slug, k) for k in (u'query', u'query_version', u'version',
                                                  u'resource_ids', u'resource_ids_and_versions')}
    result[u'created'] = found_slug.created.isoformat()
    return result


@action(schema.datastore_field_autocomplete(), help.datastore_field_autocomplete,
        toolkit.side_effect_free)
def datastore_field_autocomplete(context, text=u'', resource_ids=None, lowercase=False):
    '''
    Given a text value, finds fields that contain the given text from the given resource (or all
    resource if no resources are passed).

    :param context: the context dict from the action call
    :param text: the text to search with (default is an empty string)
    :param resource_ids: a list of resources to find fields from, if None (the default) all resource
                         fields are searched
    :param lowercase: whether to do a lowercase check or not, essentially whether to be case
                      insensitive. Default: True, be case insensitive.
    :return: the fields and the resources they came from as a dict
    '''
    # figure out which resources should be searched
    resource_ids = get_available_datastore_resources(context, resource_ids)
    if not resource_ids:
        raise toolkit.ValidationError(u"The requested resources aren't accessible to this user")

    # just get the public index mappings for the requested resource ids
    resource_ids = u','.join(map(prefix_resource, resource_ids))
    mappings = common.ES_CLIENT.indices.get_mapping(resource_ids)

    fields = defaultdict(dict)

    for index, mapping in mappings.items():
        resource_id = unprefix_index(index)

        for field_path, config in iter_data_fields(mapping):
            if any(text in (part.lower() if lowercase else part) for part in field_path):
                fields[u'.'.join(field_path)][resource_id] = {
                    u'type': config[u'type'],
                    u'fields': {f: c[u'type'] for f, c in config.get(u'fields', {}).items()}
                }

    return {
        u'count': len(fields),
        u'fields': fields,
    }
