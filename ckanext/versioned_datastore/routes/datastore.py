# !/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-versioned-datastore
# Created by the Natural History Museum in London, UK

from flask import Blueprint

from ckan.plugins import toolkit
from ..lib.stats import get_all_stats

blueprint = Blueprint(name=u'datastore', import_name=__name__, url_prefix=u'')


@blueprint.route(u'/dataset/<package_name>/resource_data/<resource_id>', methods=[u'GET', u'POST'])
def resource_data(package_name, resource_id):
    '''
    Produces the DataStore page on a resource. This page contains details of the resource's
    ingestion and indexing.
    '''
    try:
        # first, check access
        toolkit.check_access(u'resource_update', {}, {
            u'id': resource_id
        })
        # then retrieve the package and resource data
        toolkit.c.pkg_dict = toolkit.get_action(u'package_show')({}, {
            u'id': package_name
        })
        toolkit.c.resource = toolkit.get_action(u'resource_show')({}, {
            u'id': resource_id
        })
    except toolkit.ObjectNotFound:
        toolkit.abort(404, toolkit._(u'Resource not found'))
    except toolkit.NotAuthorized:
        toolkit.abort(401, toolkit._(u'Unauthorized to edit this resource'))

    if toolkit.request.method == u'POST':
        toolkit.get_action(u'datastore_reindex')({}, {
            u'resource_id': resource_id
        })
        toolkit.h.flash_success(toolkit._(u'Reindexing submitted, this may take a few minutes. You'
                                          u'can monitor progress below'))
        # redirect the user back to the page. This ensures they can do things like reload
        # without getting a pesky "would you like to resubmit this form" notice
        return toolkit.redirect_to(u'datastore.resource_data', package_name=package_name,
                                   resource_id=resource_id, _code=303)
    else:
        extra_vars = {
            u'stats': get_all_stats(resource_id),
            u'reindex_action': toolkit.url_for(u'datastore.resource_data',
                                               package_name=package_name, resource_id=resource_id),
            u'pkg_dict': toolkit.c.pkg_dict,
            u'resource': toolkit.c.resource
        }
        return toolkit.render(u'package/resource_data.html', extra_vars=extra_vars)
