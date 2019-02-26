import logging

from eevee.utils import to_timestamp

from ckan import plugins, model, logic
from ckan.model import DomainObjectOperation
from ckanext.versioned_datastore.controllers.datastore import ResourceDataController
from ckanext.versioned_datastore.lib.utils import is_datastore_resource, setup_eevee
from ckanext.versioned_datastore.logic import action, auth

log = logging.getLogger(__name__)


class VersionedSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.ITemplateHelpers, inherit=True)
    plugins.implements(plugins.IResourceController)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IRoutes, inherit=True)

    # IActions
    def get_actions(self):
        return {
            u'datastore_create': action.datastore_create,
            u'datastore_upsert': action.datastore_upsert,
            u'datastore_delete': action.datastore_delete,
            u'datastore_search': action.datastore_search,
            u'datastore_get_record_versions': action.datastore_get_record_versions,
            u'datastore_get_resource_versions': action.datastore_get_resource_versions,
            u'datastore_autocomplete': action.datastore_autocomplete,
            u'datastore_reindex': action.datastore_reindex,
            u'datastore_query_extent': action.datastore_query_extent,
        }

    # IAuthFunctions
    def get_auth_functions(self):
        return {
            u'datastore_create': auth.datastore_create,
            u'datastore_upsert': auth.datastore_upsert,
            u'datastore_delete': auth.datastore_delete,
            u'datastore_search': auth.datastore_search,
            u'datastore_get_record_versions': auth.datastore_get_record_versions,
            u'datastore_get_resource_versions': auth.datastore_get_resource_versions,
            u'datastore_autocomplete': auth.datastore_autocomplete,
            u'datastore_reindex': auth.datastore_reindex,
            u'datastore_query_extent': auth.datastore_query_extent,
        }

    # ITemplateHelpers
    def get_helpers(self):
        return {
            u'is_datastore_resource': is_datastore_resource
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
        resource_dict[u'datastore_active'] = is_datastore_resource(resource_dict['id'])
        return resource_dict

    # IResourceUrlChange and IDomainObjectModification
    def notify(self, entity, operation=None):
        '''
        Respond to changes to model objects and resource URLs. We use this hook to ensure any new
        data is imported into the versioned datastore. We're only interested in:

            - resource deletions
            - new resources
            - resources that have had their resource URL changed (i.e. have a new version of the
              data)

        :param entity: the entity that has changed
        :param operation: the operation undertaken on the object. If the function is being called
                          from IDomainObjectModification this will be one of the options from the
                          DomainObjectOperation enum, otherwise it will be None as the
                          IResourceUrlChnage version of the notify hook doesn't pass an operation,
                          just the entity that has changed.
        '''
        # we only care about resources
        if isinstance(entity, model.Resource):
            # the resource has been or is now deleted, make sure the datastore is updated
            if operation == DomainObjectOperation.changed and entity.state == u'deleted':
                logic.get_action(u'datastore_delete')({}, {u'resource_id': entity.id})
            # either the resource is new or its URL has changed
            elif operation == DomainObjectOperation.new or operation is None:
                try:
                    # trigger the datastore create action to set things up
                    created = logic.get_action(u'datastore_create')({}, {u'resource_id': entity.id})
                    if created:
                        # if the datastore index for this resource was created or already existed
                        # then load the data
                        data_dict = {
                            u'resource_id': entity.id,
                            # use replace True to replace the existing data (this is what users
                            # would expect)
                            u'replace': True,
                        }
                        # use the entities' last modified data if there is one, otherwise don't pass
                        # one and let the action default it
                        if entity.last_modified is not None:
                            data_dict[u'version'] = to_timestamp(entity.last_modified)
                        logic.get_action(u'datastore_upsert')({}, data_dict)
                except plugins.toolkit.ValidationError, e:
                    # if anything went wrong we want to catch error instead of raising otherwise
                    # resource save will fail with a 500
                    log.critical(e)
                    pass

    # IConfigurer
    def update_config(self, config):
        # add templates
        plugins.toolkit.add_template_directory(config, u'theme/templates')

    # IRoutes
    def before_map(self, map):
        map.connect(u'resource_data', u'/dataset/{package_name}/resource_data/{resource_id}',
                    controller=ResourceDataController.path, action=u'resource_data',
                    ckan_icon=u'cloud-upload')
        return map

    # IConfigurable
    def configure(self, ckan_config):
        setup_eevee(ckan_config)
