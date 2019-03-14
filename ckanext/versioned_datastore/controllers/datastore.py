from ckan import logic
from ckan.common import _
from ckan.lib import base
from ckan.lib.helpers import url_for, flash_success
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.stats import get_all_stats
from pylons.controllers.util import redirect_to


class ResourceDataController(base.BaseController):
    path = u'ckanext.versioned_datastore.controllers.datastore:ResourceDataController'

    def resource_data(self, package_name, resource_id):
        try:
            toolkit.c.pkg_dict = toolkit.get_action(u'package_show')(None, {u'id': package_name})
            toolkit.c.resource = toolkit.get_action(u'resource_show')(None, {u'id': resource_id})
        except logic.NotFound:
            base.abort(404, _(u'Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _(u'Unauthorized to edit this resource'))

        if toolkit.request.method == u'POST':
            logic.get_action(u'datastore_reindex')({}, {u'resource_id': resource_id})
            flash_success(_(u'Reindexing submitted, this may take a few minutes. You can monitor '
                            u'progress below'))
            # redirect the user back to the page. This ensures they can do things like reload
            # without getting a pesky "would you like to resubmit this form" notice
            redirect_to(u'resource_data', package_name=package_name, resource_id=resource_id,
                        _code=303)
        else:
            extra_vars = {
                u'stats': get_all_stats(resource_id),
                u'reindex_action': url_for(u'resource_data', action=u'resource_data',
                                           package_name=package_name, resource_id=resource_id),
            }
            return base.render(u'package/resource_data.html', extra_vars=extra_vars)
