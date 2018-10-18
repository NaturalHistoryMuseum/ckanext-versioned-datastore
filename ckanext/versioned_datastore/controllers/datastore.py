from ckan import logic
from ckan.lib import base
from ckan.plugins import toolkit


class ResourceDataController(base.BaseController):

    def resource_data(self, package_name, resource_id):
        try:
            toolkit.c.pkg_dict = toolkit.get_action(u'package_show')(None, {u'id': package_name})
            toolkit.c.resource = toolkit.get_action(u'resource_show')(None, {u'id': resource_id})
        except logic.NotFound:
            base.abort(404, _(u'Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _(u'Unauthorized to edit this resource'))

        return base.render(u'package/resource_data.html')
