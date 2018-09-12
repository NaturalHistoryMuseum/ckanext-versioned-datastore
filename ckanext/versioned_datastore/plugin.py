from ckan import plugins
from ckanext.datastore.logic import auth
from ckanext.versioned_datastore.lib.utils import is_datastore_resource
from ckanext.versioned_datastore.logic import action


class VersionedSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.interfaces.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.ITemplateHelpers, inherit=True)

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
        # for the moment, just return the auth functions used by the datastore
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
