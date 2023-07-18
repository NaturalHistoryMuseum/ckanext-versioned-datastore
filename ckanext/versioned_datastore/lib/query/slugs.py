import datetime
import hashlib
import random
import logging

from ckan import model
from ckan.plugins import toolkit
from sqlalchemy.exc import IntegrityError

from .schema import get_latest_query_version, hash_query
from .schema import validate_query
from .slug_words import list_one, list_two, list_three
from .utils import get_available_datastore_resources, get_resources_and_versions
from ...model.slugs import DatastoreSlug, NavigationalSlug

log = logging.getLogger(__name__)


def generate_query_hash(
    query, query_version, version, resource_ids, resource_ids_and_versions
):
    """
    Given a query and the parameters required to run it, create a unique id for it (a
    hash) and returns it. The hashing algorithm used is sha1 and the output will be a 40
    character hexidecimal string.

    :param query: the query dict
    :param query_version: the query version
    :param version: the data version
    :param resource_ids: the ids of the resources under search
    :param resource_ids_and_versions: the resource ids and specific versions to search at for them
    :return: a unique id for the query, which is a hash of the query and parameters
    """
    hash_value = hashlib.sha1()
    bits = [
        hash_query(query, query_version),
        query_version,
        version,
        # sort the resource ids to ensure stability
        sorted(resource_ids) if resource_ids is not None else None,
        # sort the resource ids and versions to ensure stability
        sorted(resource_ids_and_versions.items())
        if resource_ids_and_versions is not None
        else None,
    ]
    hash_value.update('|'.join(map(str, bits)).encode('utf-8'))
    return hash_value.hexdigest()


def generate_pretty_slug(word_lists=(list_one, list_two, list_three)):
    """
    Generate a new slug using the adjective and animal lists available. The default
    word_lists value is a trio of lists: (adjectives, adjectives, animals). This
    produces >31,000,000 unique combinations which should be more than enough! This
    function does have the potential to produce duplicate adjectives (for example,
    green-green-llama) but the chances are really low and it doesn't really matter.

    :param word_lists: a sequence of word lists to choose from
    :return: the slug
    """
    return '-'.join(map(random.choice, word_lists))


def create_slug(
    context,
    query,
    query_version,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
    pretty_slug=True,
    attempts=5,
):
    """
    Creates a new slug in the database and returns the saved DatastoreSlug object. If a
    slug already exists under the query hash then the existing slug entry is returned.
    In addition to the slug object, also returned is information about whether the slug
    was new or not.

    Only valid queries get a slug, otherwise we raise a ValidationError.

    Only valid resource ids included in the list will be stored, any invalid ones will be excluded.
    If a list of resource ids is provided and none of the requested resource ids are valid, then a
    ValidationError is raised.

    :param context: the context dict so that we can check the validity of any resources
    :param query: the query dict
    :param query_version: the query version in use
    :param version: the version to search at
    :param resource_ids: the resources to search (a list)
    :param resource_ids_and_versions: the resources and versions to search at (a dict)
    :param pretty_slug: whether to generate a pretty slug or just use the uuid id of the slug, by
                        default this is True
    :param attempts: how many times to try creating a pretty slug, default: 5
    :return: a 2-tuple containing a boolean indicating whether the slug object returned was newly
             created and the DatastoreSlug object itself. If we couldn't create a slug object for
             some reason then (False, None) is returned.
    """
    # only store valid queries!
    validate_query(query, query_version)

    resource_ids, resource_ids_and_versions = get_resources_and_versions(
        resource_ids, resource_ids_and_versions, version
    )

    query_hash = generate_query_hash(
        query, query_version, version, resource_ids, resource_ids_and_versions
    )

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
            query=query,
            query_version=query_version,
            version=version,
            resource_ids=resource_ids,
            resource_ids_and_versions=resource_ids_and_versions,
        )

        if pretty_slug:
            new_slug.pretty_slug = generate_pretty_slug()

        try:
            new_slug.save()
            break
        except IntegrityError:
            if pretty_slug:
                # assume this failed because of the pretty slug needing to be unique and try again
                model.Session.rollback()
                continue
            else:
                # something else has happened here
                raise
    else:
        return False, None

    return True, new_slug


def create_nav_slug(
    context, query, version=None, resource_ids=None, resource_ids_and_versions=None
):
    try:
        # clear old slugs before we make new ones
        clean_nav_slugs()
    except Exception as e:
        # if it fails, log it and move on
        log.debug(f'Cleaning nav slugs failed: {e}')

    query_version = get_latest_query_version()  # it should always be the latest version
    validate_query(query, query_version)

    resource_ids, resource_ids_and_versions = get_resources_and_versions(
        resource_ids, resource_ids_and_versions, version
    )

    query_hash = generate_query_hash(
        query, query_version, version, resource_ids, resource_ids_and_versions
    )

    existing_slug = (
        model.Session.query(NavigationalSlug)
        .filter(NavigationalSlug.query_hash == query_hash)
        .first()
    )

    if existing_slug is not None:
        return False, existing_slug

    new_slug = NavigationalSlug(
        query_hash=query_hash,
        query=query,
        resource_ids_and_versions=resource_ids_and_versions,
    )
    new_slug.save()

    return True, new_slug


def resolve_slug(slug, allow_nav=True):
    """
    Resolves the given slug and returns it if it's found, otherwise None is returned.

    :param slug: the slug
    :param allow_nav: allow resolving to a navigational slug
    :return: a DatastoreSlug object or None if the slug couldn't be found
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


def reserve_slug(
    reserved_pretty_slug,
    query=None,
    query_version=None,
    version=None,
    resource_ids=None,
    resource_ids_and_versions=None,
):
    """
    This function can be used to reserve a slug using a specific string. This should
    probably only be called during this extension's initialisation via the
    datastore_reserve_slugs interface function.

    If a slug already exists in the database with the same reserved pretty slug and the same
    query parameters then nothing happens.

    If a slug already exists in the database with the same reserved pretty slug but a different
    set of query parameters then a DuplicateSlugException is raised.

    If a slug already exists in the database with the same query parameters but no reserved
    pretty slug then the reserved pretty slug is added to the slug.

    :param reserved_pretty_slug: the slug string to reserve
    :param query: the query dict
    :param query_version: the query schema version
    :param version: the version of the data
    :param resource_ids: the resource ids to search
    :param resource_ids_and_versions: the resources ids and specific versions for each to search
    :return: a DatastoreSlug object that has either been found (if it already existed), created (if
             no slug existed) or updated (if a slug existed for the query parameters, but no
             reserved query string was associated with it).
    """
    # default some parameters and then assert they are all the right types. We do this because if
    # there are problems they're going to be reported back to the developer not the user
    if query is None:
        query = {}
    if query_version is None:
        query_version = get_latest_query_version()
    assert isinstance(query, dict)
    assert isinstance(query_version, str)
    if version is not None:
        assert isinstance(version, int)
    if resource_ids is not None:
        assert isinstance(resource_ids, list)
    if resource_ids_and_versions is not None:
        assert isinstance(resource_ids_and_versions, dict)

    slug = resolve_slug(reserved_pretty_slug, False)
    if slug is not None:
        # a slug with this reserved pretty slug already exists
        return slug
    else:
        # there is no slug associated with this reserved pretty slug so let's see if there's a slug
        # using the query parameters
        query_hash = generate_query_hash(
            query, query_version, version, resource_ids, resource_ids_and_versions
        )
        slug = (
            model.Session.query(DatastoreSlug)
            .filter(DatastoreSlug.query_hash == query_hash)
            .first()
        )
        if slug is None:
            # we need to make a new slug
            context = {'ignore_auth': True}
            success, slug = create_slug(
                context,
                query,
                query_version,
                version,
                resource_ids,
                resource_ids_and_versions,
                pretty_slug=False,
            )
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


def clean_nav_slugs(before=None):
    """
    Delete old/expired navigational slugs.

    :param before: a datetime object; slugs created before this time will be removed
                   (defaults to 2 days ago)
    :return: the number of deleted slugs
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
