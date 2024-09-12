from typing import Optional, List

import jsonschema
from ckantools.decorators import action

from ckan.plugins import toolkit, plugin_loaded
from ckanext.versioned_datastore.lib.query.schema import (
    get_latest_query_version,
    InvalidQuerySchemaVersionError,
)
from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.query.slugs.slugs import (
    create_nav_slug,
    create_slug,
    resolve_slug,
)
from ckanext.versioned_datastore.lib.query.utils import convert_to_multisearch
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
    if query_version and query_version.lower().startswith("v0"):
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
        raise toolkit.ValidationError("Failed to generate new slug")

    return {
        "slug": slug.get_slug_string(),
        "is_new": is_new,
        "is_reserved": (
            False if nav_slug else slug.reserved_pretty_slug == slug.get_slug_string()
        ),
    }


@action(schema.vds_slug_resolve(), helptext.vds_slug_resolve, get=True)
def vds_slug_resolve(slug: str):
    # try resolving the slug first
    resolved = resolve_slug(slug)
    if resolved:
        result = {
            "query": resolved.query,
            "query_version": resolved.query_version,
            "version": resolved.version,
            "resource_ids": resolved.resource_ids,
            "created": resolved.created.isoformat(),
        }
        if result.get("query_version") == "v0":
            result["query"] = convert_to_multisearch(result["query"])
            result["query_version"] = get_latest_query_version()
        return result

    # then check if it's a query DOI
    if plugin_loaded("query_dois"):
        from ckanext.query_dois.model import QueryDOI
        from ckan import model

        resolved = model.Session.query(QueryDOI).filter(QueryDOI.doi == slug).first()
        if resolved:
            if resolved.query_version == "v0":
                query = convert_to_multisearch(resolved.query)
                query_version = get_latest_query_version()
            else:
                query = resolved.query
                query_version = resolved.query_version
            return {
                "query": query,
                "query_version": query_version,
                "version": resolved.requested_version,
                "resource_ids": resolved.get_resource_ids(),
                "created": resolved.timestamp.isoformat(),
            }

    # if both slug and DOI have failed
    raise toolkit.ValidationError("Slug not found")


@action(schema.vds_slug_edit(), helptext.vds_slug_edit)
def vds_slug_edit(context: dict, current_slug: str, new_reserved_slug: str):
    slug = resolve_slug(current_slug)
    if slug is None:
        raise toolkit.Invalid(f"The slug {current_slug} does not exist")
    if slug.reserved_pretty_slug and not context["auth_user_obj"].sysadmin:
        raise toolkit.NotAuthorized(
            "Only sysadmins can replace existing reserved slugs."
        )
    slug.reserved_pretty_slug = new_reserved_slug.lower()
    slug.commit()
    return slug.as_dict()
