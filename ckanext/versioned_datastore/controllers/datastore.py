from ckan import logic
from ckan.common import _
from ckan.lib import base
from ckan.lib.helpers import url_for
from ckan.plugins import toolkit
from ckanext.versioned_datastore.lib.stats import get_all_stats


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

        reindex_result = {}
        if toolkit.request.method == u'POST':
            try:
                reindex_result = logic.get_action(u'datastore_reindex')({}, {
                    u'resource_id': resource_id})
            except logic.ValidationError:
                pass

        extra_vars = {
            u'stats': get_all_stats(resource_id),
            u'reindex_action': url_for(u'resource_data', action=u'resource_data',
                                       package_name=package_name, resource_id=resource_id),
            u'reindex_result': reindex_result,
        }
        return base.render(u'package/resource_data.html', extra_vars=extra_vars)
