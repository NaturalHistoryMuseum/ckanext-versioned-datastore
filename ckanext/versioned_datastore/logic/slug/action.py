from typing import List, Optional

import jsonschema
from ckan.plugins import plugin_loaded, toolkit
from ckantools.decorators import action

from ckanext.versioned_datastore.lib.query.schema import (
    InvalidQuerySchemaVersionError,
    get_latest_query_version,
)
from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.query.slugs.slugs import (
    create_nav_slug,
    create_slug,
    resolve_slug,
)
from ckanext.versioned_datastore.lib.query.utils import convert_to_multisearch
from ckanext.versioned_datastore.lib.utils import get_available_datastore_resources
from ckanext.versioned_datastore.logic.slug import helptext, schema


@action(schema.vds_slug_create(), helptext.vds_slug_create)
def vds_slug_create(
    resource_ids: List[str] = None,
    version: Optional[int] = None,
    query: Optional[dict] = None,
    query_version: Optional[str] = None,
    pretty_slug: bool = True,
    nav_slug: bool = False,
):
    """
    Creates a slug for the given query options. If the slug already exists then this
    will be indicated in the is_new key of the response dict.

    :param resource_ids: the resource IDs to search over (no value or an empty list will
        result in a search over all resources at the time of recovering the slug and
        searching)
    :param version: the version to search at
    :param query: the query to search with
    :param query_version: the query schema version to use
    :param pretty_slug: whether to produce a pretty slug (es muy bonita)
    :param nav_slug: whether to produce a nav type slug
    :returns: details about the slug
    """
    if query_version and query_version.lower().startswith('v0'):
        # this is an old/basic query, so we need to convert it first
        query = convert_to_multisearch(query)
        query_version = None

    schema_query = SchemaQuery(resource_ids, version, query, query_version)
    try:
        # check the query is valid before we go any further
        schema_query.validate()
    except jsonschema.ValidationError as e:
        raise toolkit.ValidationError(e.message)

    try:
        if nav_slug:
            is_new, slug = create_nav_slug(schema_query)
        else:
            is_new, slug = create_slug(schema_query, pretty_slug=pretty_slug)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    if slug is None:
        raise toolkit.ValidationError('Failed to generate new slug')

    return {
        'slug': slug.get_slug_string(),
        'is_new': is_new,
        'is_reserved': (
            False if nav_slug else slug.reserved_pretty_slug == slug.get_slug_string()
        ),
    }


@action(schema.vds_slug_resolve(), helptext.vds_slug_resolve, get=True)
def vds_slug_resolve(slug: str):
    """
    Resolves the given slug and returns the query details. If the provided slug string
    is a DOI and the query_dois plugin is loaded, this action also checks that plugin
    for DOIs and returns the query details of the DOI.

    :param slug: the slug to resolve
    :returns: the slug query details, or a ValidationError if no slug could be resolved
    """
    saved_search_type = None
    result = {
        'query': {},
        'query_version': '',
        'version': None,
        'resource_ids': [],
        'created': '',
        'warnings': [],
    }
    # try resolving the slug first
    resolved = resolve_slug(slug)
    if resolved:
        saved_search_type = 'slug'
        result.update(
            {
                'query': resolved.query,
                'query_version': resolved.query_version,
                'version': resolved.version,
                # todo: should we turn an empty resource_ids list into the list of all
                #       available resources at this point or let it ride until query time?
                'resource_ids': resolved.resource_ids,
                'created': resolved.created.isoformat(),
            }
        )

    # then check if it's a query DOI
    elif plugin_loaded('query_dois'):
        from ckan import model

        from ckanext.query_dois.model import QueryDOI

        resolved = model.Session.query(QueryDOI).filter(QueryDOI.doi == slug).first()
        if resolved:
            saved_search_type = 'doi'
            if resolved.requested_version:
                requested_version = resolved.requested_version
            else:
                # this should only be applicable for multisearch DOIs created before VDS
                # v6, which allowed different versions per resource; this rounds them
                # all up to  the newest version in the list. See #187 for more detail.
                requested_version = max(resolved.get_rounded_versions())
            result.update(
                {
                    'query': resolved.query,
                    'query_version': resolved.query_version,
                    'version': requested_version,
                    'resource_ids': resolved.get_resource_ids(),
                    'created': resolved.timestamp.isoformat(),
                }
            )

    if saved_search_type is None:
        # if both slug and DOI have failed
        raise toolkit.ValidationError('This saved search could not be found.')

    if result.get('query_version') == 'v0':
        result['query'] = convert_to_multisearch(result['query'])
        result['query_version'] = get_latest_query_version()

    # warn that "all resources" changes
    if len(result['resource_ids']) == 0:
        result['warnings'].append(
            toolkit._(
                'Resource IDs were not saved for this search. The list of available '
                'resources may have changed since this slug was saved.'
            )
        )

    # check that all the resources exist and are available
    all_resource_ids = get_available_datastore_resources(
        ignore_auth=False, user_id=getattr(toolkit.g, 'user', '')
    )
    valid_resource_ids = list(set(result['resource_ids']) & all_resource_ids)
    valid_resource_count = len(valid_resource_ids)
    current_resource_count = len(result['resource_ids'])
    if valid_resource_count != current_resource_count:
        if valid_resource_count == 0:
            # this would change the search too much
            raise toolkit.ValidationError(
                'All resources associated with this search have been '
                'deleted, moved, or are no longer available.'
            )
        result['warnings'].append(
            toolkit._(
                'Some resources have been deleted, moved, or are no longer available. '
                'Affected resources: '
            )
            + str(current_resource_count - valid_resource_count)
        )
        if saved_search_type == 'doi' and hasattr(resolved, 'count'):
            result['warnings'].append(
                toolkit._('Record count at save time: ') + str(resolved.count)
            )
    result['resource_ids'] = valid_resource_ids

    return result


@action(schema.vds_slug_reserve(), helptext.vds_slug_reserve)
def vds_slug_reserve(context: dict, current_slug: str, new_reserved_slug: str):
    """
    Attempts to reserve the given slug under a new name. Each slug can only have one
    reserved name so if a reserved name already exists, the new name will overwrite it.

    Note that only sysadmins can update a reserved slug name, but normal users can add a
    reserved name.

    :param context: the CKAN action context
    :param current_slug: the current slug name
    :param new_reserved_slug: the new slug name
    :returns: the slug details as a dict
    """
    slug = resolve_slug(current_slug)
    if slug is None:
        raise toolkit.ValidationError(f'The slug {current_slug} does not exist')
    if slug.reserved_pretty_slug and not context['auth_user_obj'].sysadmin:
        raise toolkit.NotAuthorized(
            'Only sysadmins can replace existing reserved slugs.'
        )
    slug.reserved_pretty_slug = new_reserved_slug.lower()
    slug.commit()
    return slug.as_dict()
