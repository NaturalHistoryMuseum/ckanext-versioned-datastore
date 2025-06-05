import datetime
import hashlib
import logging
import random
from typing import Optional, Tuple

from ckan import model
from sqlalchemy.exc import IntegrityError

from ckanext.versioned_datastore.model.slugs import DatastoreSlug, NavigationalSlug

from ..schema import hash_query
from ..search.query import SchemaQuery
from .slug_words import list_one, list_three, list_two

log = logging.getLogger(__name__)


def generate_query_hash(query: SchemaQuery) -> str:
    """
    Given a query and the parameters required to run it, create a unique id for it (a
    hash) and returns it. The hashing algorithm used is sha1 and the output will be a 40
    character hexidecimal string.

    :returns: a unique id for the query, which is a hash of the query and parameters
    """
    query.validate()
    hash_value = hashlib.sha1()
    bits = [
        hash_query(query.query, query.query_version),
        query.query_version,
        query.version,
        # sort the resource ids to ensure stability
        sorted(query.resource_ids),
    ]
    hash_value.update('|'.join(map(str, bits)).encode('utf-8'))
    return hash_value.hexdigest()


def generate_pretty_slug(word_lists=(list_one, list_two, list_three)) -> str:
    """
    Generate a new slug using the adjective and animal lists available. The default
    word_lists value is a trio of lists: (adjectives, adjectives, animals). This
    produces >31,000,000 unique combinations which should be more than enough! This
    function does have the potential to produce duplicate adjectives (for example,
    green-green-llama) but the chances are really low and it doesn't really matter.

    :param word_lists: a sequence of word lists to choose from
    :returns: the slug
    """
    return '-'.join(map(random.choice, word_lists))


def create_slug(
    query: SchemaQuery,
    pretty_slug=True,
    attempts=5,
) -> Tuple[bool, Optional[DatastoreSlug]]:
    """
    Creates a new slug in the database and returns the saved DatastoreSlug object. If a
    slug already exists under the query hash then the existing slug entry is returned.
    In addition to the slug object, also returned is information about whether the slug
    was new or not.

    :param pretty_slug: whether to generate a pretty slug or just use the uuid id of the
        slug, by default this is True
    :param attempts: how many times to try creating a pretty slug, default: 5
    :returns: a 2-tuple containing a boolean indicating whether the slug object returned
        was newly created and the DatastoreSlug object itself. If we couldn't create a
        slug object for some reason then (False, None) is returned.
    """
    query_hash = generate_query_hash(query)

    existing_slug = (
        model.Session.query(DatastoreSlug)
        .filter(DatastoreSlug.query_hash == query_hash)
        .first()
    )

    if existing_slug is not None:
        return False, existing_slug

    while attempts:
        attempts -= 1
        new_slug = DatastoreSlug(
            query_hash=query_hash,
            query=query.query,
            query_version=query.query_version,
            version=query.version,
            resource_ids=query.resource_ids,
        )

        if pretty_slug:
            new_slug.pretty_slug = generate_pretty_slug()

        try:
            new_slug.save()
            break
        except IntegrityError:
            if pretty_slug:
                # assume this failed because of the pretty slug needing to be unique and
                # try again
                model.Session.rollback()
                continue
            else:
                # something else has happened here
                raise
    else:
        return False, None

    return True, new_slug


def create_nav_slug(query: SchemaQuery) -> Tuple[bool, DatastoreSlug]:
    try:
        # clear old slugs before we make new ones
        clean_nav_slugs()
    except Exception as e:
        # if it fails, log it and move on
        log.debug(f'Cleaning nav slugs failed: {e}')

    query_hash = generate_query_hash(query)

    existing_slug = (
        model.Session.query(NavigationalSlug)
        .filter(NavigationalSlug.query_hash == query_hash)
        .first()
    )

    if existing_slug is not None:
        return False, existing_slug

    new_slug = NavigationalSlug(
        query_hash=query_hash,
        query=query.query,
        resource_ids=query.resource_ids,
    )
    new_slug.save()

    return True, new_slug


def resolve_slug(slug, allow_nav=True) -> Optional[DatastoreSlug]:
    """
    Resolves the given slug and returns it if it's found, otherwise None is returned.

    :param slug: the slug
    :param allow_nav: allow resolving to a navigational slug
    :returns: a DatastoreSlug object or None if the slug couldn't be found
    """
    if slug.startswith(NavigationalSlug.prefix) and allow_nav:
        try:
            # clean old slugs because we don't want old ones to continue resolving
            clean_nav_slugs()
        except Exception as e:
            # if it fails, log it and move on
            log.debug(f'Cleaning nav slugs failed: {e}')
        return (
            model.Session.query(NavigationalSlug)
            .filter(NavigationalSlug.on_slug(slug))
            .first()
        )
    return (
        model.Session.query(DatastoreSlug).filter(DatastoreSlug.on_slug(slug)).first()
    )


class DuplicateSlugException(Exception):
    pass


def reserve_slug(reserved_pretty_slug: str, query: SchemaQuery) -> DatastoreSlug:
    """
    This function can be used to reserve a slug using a specific string. This should
    probably only be called during this extension's initialisation via the
    vds_reserve_slugs interface function.

    If a slug already exists in the database with the same reserved pretty slug and the
    same query parameters then nothing happens.

    If a slug already exists in the database with the same reserved pretty slug but a
    different set of query parameters then a DuplicateSlugException is raised.

    If a slug already exists in the database with the same query parameters but no
    reserved pretty slug then the reserved pretty slug is added to the slug.

    :param reserved_pretty_slug: the slug string to reserve
    :returns: a DatastoreSlug object that has either been found (if it already existed),
        created (if no slug existed) or updated (if a slug existed for the query
        parameters, but no reserved query string was associated with it).
    """
    slug = resolve_slug(reserved_pretty_slug, False)
    if slug is not None:
        # a slug with this reserved pretty slug already exists
        return slug
    else:
        # there is no slug associated with this reserved pretty slug so let's see if
        # there's a slug using the query parameters
        query_hash = generate_query_hash(query)
        slug = (
            model.Session.query(DatastoreSlug)
            .filter(DatastoreSlug.query_hash == query_hash)
            .first()
        )
        if slug is None:
            # we need to make a new slug
            success, slug = create_slug(query, pretty_slug=False)
            if not success:
                # this should never really happen
                raise Exception('Failed to create new reserved slug')
            slug.reserved_pretty_slug = reserved_pretty_slug
            slug.save()
            return slug
        else:
            # see if we can update the slug that already exists
            if slug.reserved_pretty_slug is None:
                slug.reserved_pretty_slug = reserved_pretty_slug
                slug.save()
                return slug
            else:
                raise DuplicateSlugException(
                    f'The query parameters are already associated with a '
                    f'different slug: {slug.get_slug_string()}'
                )


def clean_nav_slugs(before=None) -> int:
    """
    Delete old/expired navigational slugs.

    :param before: a datetime object; slugs created before this time will be removed
        (defaults to 2 days ago)
    :returns: the number of deleted slugs
    """
    if before is None:
        before = datetime.datetime.utcnow() - datetime.timedelta(days=2)

    old_slugs = (
        model.Session.query(NavigationalSlug)
        .filter(NavigationalSlug.created < before)
        .all()
    )
    slug_count = len(old_slugs)
    for slug in old_slugs:
        slug.delete()

    model.Session.commit()

    return slug_count
