import logging

from ckanext.versioned_datastore.lib import utils
from eevee.utils import to_timestamp

from ckan import model
from ckan.plugins import toolkit, interfaces, SingletonPlugin, implements
from ckan.model import DomainObjectOperation
from ckanext.versioned_datastore.controllers.datastore import ResourceDataController
from ckanext.versioned_datastore.lib.utils import is_datastore_resource, setup_eevee
from ckanext.versioned_datastore.logic import action, auth

log = logging.getLogger(__name__)


class VersionedSearchPlugin(SingletonPlugin):
    implements(interfaces.IActions)
    implements(interfaces.IAuthFunctions)
    implements(interfaces.ITemplateHelpers, inherit=True)
    implements(interfaces.IResourceController, inherit=True)
    implements(interfaces.IDomainObjectModification, inherit=True)
    implements(interfaces.IConfigurer)
    implements(interfaces.IConfigurable)
    implements(interfaces.IRoutes, inherit=True)

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
            u'datastore_get_rounded_version': action.datastore_get_rounded_version,
            u'datastore_search_raw': action.datastore_search_raw,
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
            u'datastore_get_rounded_version': auth.datastore_get_rounded_version,
            u'datastore_search_raw': auth.datastore_search_raw,
        }

    # ITemplateHelpers
    def get_helpers(self):
        return {
            u'is_datastore_resource': is_datastore_resource
        }

    # IResourceController
    def before_show(self, resource_dict):
        resource_dict[u'datastore_active'] = is_datastore_resource(resource_dict[u'id'])
        return resource_dict

    # IDomainObjectModification
    def notify(self, entity, operation):
        '''
        Respond to changes to model objects. We use this hook to ensure any new data is imported
        into the versioned datastore and to make sure the privacy settings on the data are up to
        date. We're only interested in:

            - resource deletions
            - new resources
            - resources that have had changes to their URL
            - packages that have changed

        :param entity: the entity that has changed
        :param operation: the operation undertaken on the object. This will be one of the options
                          from the DomainObjectOperation enum.
        '''
        if isinstance(entity, model.Package) and operation == DomainObjectOperation.changed:
            # if a package is the target entity and it's been changed ensure the privacy is applied
            # correctly to it's resource indexes
            utils.update_resources_privacy(entity)
        elif isinstance(entity, model.Resource):
            context = {u'model': model, u'ignore_auth': True}
            data_dict = {u'resource_id': entity.id}
            do_upsert = False

            # use the entities' last modified data if there is one, otherwise don't pass
            # one and let the action default it
            last_modifed = getattr(entity, u'last_modified', None)
            if last_modifed is not None:
                data_dict[u'version'] = to_timestamp(last_modifed)

            if operation == DomainObjectOperation.deleted:
                logic.get_action(u'datastore_delete')(context, {u'resource_id': entity.id})
            elif operation == DomainObjectOperation.new:
                # datastore_create returns True when the resource looks like it's ingestible
                do_upsert = logic.get_action(u'datastore_create')(context, data_dict)
            elif operation == DomainObjectOperation.changed:
                # only do the upsert on changed events if the URL has changed
                do_upsert = getattr(entity, u'url_changed', False)

            if do_upsert:
                # use replace True to replace the existing data (this is what users would expect)
                data_dict[u'replace'] = True
                logic.get_action(u'datastore_upsert')(context, data_dict)

    # IConfigurer
    def update_config(self, config):
        # add templates
        toolkit.add_template_directory(config, u'theme/templates')

    # IRoutes
    def before_map(self, map):
        map.connect(u'resource_data', u'/dataset/{package_name}/resource_data/{resource_id}',
                    controller=ResourceDataController.path, action=u'resource_data',
                    ckan_icon=u'cloud-upload')
        return map

    # IConfigurable
    def configure(self, ckan_config):
        setup_eevee(ckan_config)
