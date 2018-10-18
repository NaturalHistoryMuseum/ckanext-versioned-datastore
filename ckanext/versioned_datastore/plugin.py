import logging

from ckan import plugins, model, logic
from ckanext.versioned_datastore.lib.utils import is_datastore_resource
from ckanext.versioned_datastore.logic import action, auth

log = logging.getLogger(__name__)


class VersionedSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.interfaces.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.ITemplateHelpers, inherit=True)
    plugins.implements(plugins.IResourceController)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IRoutes, inherit=True)

    # IActions
    def get_actions(self):
        return {
            'datastore_create': action.datastore_create,
            'datastore_upsert': action.datastore_upsert,
            'datastore_delete': action.datastore_delete,
            'datastore_search': action.datastore_search,
            'datastore_get_record_versions': action.datastore_get_record_versions,
            'datastore_autocomplete': action.datastore_autocomplete,
        }

    # IAuthFunctions
    def get_auth_functions(self):
        return {
            'datastore_create': auth.datastore_create,
            'datastore_upsert': auth.datastore_upsert,
            'datastore_delete': auth.datastore_delete,
            'datastore_search': auth.datastore_search,
            'datastore_get_record_versions': auth.datastore_get_record_versions,
            'datastore_autocomplete': auth.datastore_autocomplete,
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

    # IResourceUrlChange and IDomainObjectModification
    def notify(self, entity, operation=None):
        if isinstance(entity, model.Resource):
            if (operation == model.domain_object.DomainObjectOperation.changed and
                    entity.state == 'deleted'):
                # the resource has been or is now deleted, make sure the
                logic.get_action(u'datastore_delete')({}, {u'resource_id': entity.id})
            elif operation == model.domain_object.DomainObjectOperation.new or not operation:
                # if operation is None, resource URL has been changed, as the notify function in
                # IResourceUrlChange only takes 1 parameter
                try:
                    # trigger the datastore create action to set things up
                    created = logic.get_action(u'datastore_create')({}, {u'resource_id': entity.id})
                    if created:
                        # if the datastore index for this resource was created then load the data.
                        # Note that we pass through the remove index_action to make sure any new
                        # data replaces the existing data
                        logic.get_action(u'datastore_upsert')({}, {u'resource_id': entity.id,
                                                                   u'index_action': u'remove'})
                except plugins.toolkit.ValidationError, e:
                    # if anything went wrong we want to catch error instead of raising otherwise
                    # resource save will fail with 500
                    log.critical(e)
                    pass

    # IConfigurer
    def update_config(self, config):
        # add templates
        plugins.toolkit.add_template_directory(config, 'theme/templates')

    # IRoutes
    def before_map(self, map):
        map.connect(
            'resource_data', '/dataset/{package_name}/resource_data/{resource_id}',
            controller='ckanext.versioned_datastore.controllers.datastore:ResourceDataController',
            action='resource_data', ckan_icon='cloud-upload')
        return map
