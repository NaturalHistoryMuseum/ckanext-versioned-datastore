from ckan import model
from ckan.plugins import toolkit
from elasticsearch_dsl import Search

from .. import common


def get_available_datastore_resources(context, only=None):
    '''
    Returns a set of resource ids accessible to the current user based on the given context that
    are also datastore resources. If the only parameter is passed then it is used to filter the set
    of resources that are returned to include only ones in the only list. If the parameter is not
    passed, or indeed is falsey in any way (such as an empty list) then all resource ids available
    to the user are returned.

    :param context: the dict ckan context to request auth against
    :param only: optional list of resource ids to filter the returned list by. Defaults to None
                 which indicates all available resources should be returned
    :return: a set of resource ids
    '''
    # retrieve all resource ids and associated package ids direct from the database for speed
    query = model.Session.query(model.Resource).join(model.Package) \
        .filter(model.Resource.state == u'active') \
        .filter(model.Package.state == u'active') \
        .with_entities(model.Resource.id, model.Package.id)
    # retrieve the names in the status index
    status_search = Search(index=common.CONFIG.elasticsearch_status_index_name,
                           using=common.ES_CLIENT).source([u'name'])

    if only:
        # apply filters to only get the resources passed in the only list
        query = query.filter(model.Resource.id.in_(only))
        status_search = status_search.filter(u'terms', name=only)

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
                toolkit.check_access(u'package_show', context, {u'id': package_id})
                package_access_cache[package_id] = True
                # access allowed, add to the list
                resource_ids.add(resource_id)
            except toolkit.NotAuthorized:
                package_access_cache[package_id] = False
        else:
            # skip, there is no access
            continue

    return resource_ids
