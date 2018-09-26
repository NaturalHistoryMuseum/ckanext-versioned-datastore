from ckan import plugins
from ckanext.versioned_datastore.lib.utils import is_datastore_resource
from ckanext.versioned_datastore.logic import action, auth


class VersionedSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.interfaces.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.ITemplateHelpers, inherit=True)
    plugins.implements(plugins.IResourceController)

    # IActions
    def get_actions(self):
        return {
            'datastore_create': action.datastore_create,
            'datastore_upsert': action.datastore_upsert,
            'datastore_delete': action.datastore_delete,
            'datastore_search': action.datastore_search,
        }

    # IAuthFunctions
    def get_auth_functions(self):
        return {
            'datastore_create': auth.datastore_create,
            'datastore_upsert': auth.datastore_upsert,
            'datastore_delete': auth.datastore_delete,
            'datastore_search': auth.datastore_search,
        }

    # ITemplateHelpers
    def get_helpers(self):
        return {
            'is_datastore_resource': is_datastore_resource
        }

    # IResourceController
    def before_show(self, resource_dict):
        # TODO: this url business?
        # Modify the resource url of datastore resources so that
        # they link to the datastore dumps.
        # if resource_dict.get('url_type') == 'datastore':
        #     resource_dict['url'] = p.toolkit.url_for(
        #         controller='ckanext.datastore.controller:DatastoreController',
        #         action='dump', resource_id=resource_dict['id'])
        resource_dict['datastore_active'] = is_datastore_resource(resource_dict['id'])
        return resource_dict
