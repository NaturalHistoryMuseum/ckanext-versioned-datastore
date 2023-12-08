# !/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-versioned-datastore
# Created by the Natural History Museum in London, UK

from ckan.plugins import toolkit
from flask import Blueprint

from ..lib.importing.stats import get_all_stats

blueprint = Blueprint(name='datastore', import_name=__name__, url_prefix='')


@blueprint.route(
    '/dataset/<package_name>/resource_data/<resource_id>', methods=['GET', 'POST']
)
def resource_data(package_name, resource_id):
    """
    Produces the DataStore page on a resource.

    This page contains details of the resource's ingestion and indexing.
    """
    try:
        # first, check access
        toolkit.check_access('resource_update', {}, {'id': resource_id})
        # then retrieve the package and resource data
        toolkit.c.pkg_dict = toolkit.get_action('package_show')(
            {}, {'id': package_name}
        )
        toolkit.c.resource = toolkit.get_action('resource_show')(
            {}, {'id': resource_id}
        )
    except toolkit.ObjectNotFound:
        toolkit.abort(404, toolkit._('Resource not found'))
    except toolkit.NotAuthorized:
        toolkit.abort(401, toolkit._('Unauthorized to edit this resource'))

    if toolkit.request.method == 'POST':
        toolkit.get_action('datastore_reindex')({}, {'resource_id': resource_id})
        toolkit.h.flash_success(
            toolkit._(
                'Reindexing submitted, this may take a few minutes. You '
                'can monitor progress below'
            )
        )
        # redirect the user back to the page. This ensures they can do things like reload
        # without getting a pesky "would you like to resubmit this form" notice
        return toolkit.redirect_to(
            'datastore.resource_data',
            package_name=package_name,
            resource_id=resource_id,
            _code=303,
        )
    else:
        extra_vars = {
            'stats': get_all_stats(resource_id),
            'reindex_action': toolkit.url_for(
                'datastore.resource_data',
                package_name=package_name,
                resource_id=resource_id,
            ),
            'pkg_dict': toolkit.c.pkg_dict,
            'resource': toolkit.c.resource,
        }
        return toolkit.render('package/resource_data.html', extra_vars=extra_vars)
