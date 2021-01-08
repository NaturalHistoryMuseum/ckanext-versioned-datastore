import logging
from datetime import datetime

from ckan import model
from ckan.model import DomainObjectOperation
from ckan.plugins import toolkit, interfaces, SingletonPlugin, implements, PluginImplementations
from contextlib2 import suppress
from eevee.utils import to_timestamp

from . import routes, helpers, cli
from .interfaces import IVersionedDatastoreQuerySchema, IVersionedDatastore
from .lib.common import setup
from .lib.datastore_utils import is_datastore_resource, ReadOnlyResourceException, \
    InvalidVersionException, update_resources_privacy
from .lib.query.schema import register_schema
from .lib.query.v1_0_0 import v1_0_0Schema
from .logic import auth
from .logic.actions import basic_search, crud, downloads, extras, multisearch
from .logic.actions.utils import create_actions

log = logging.getLogger(__name__)


class VersionedSearchPlugin(SingletonPlugin):
    implements(interfaces.IActions)
    implements(interfaces.IAuthFunctions)
    implements(interfaces.ITemplateHelpers, inherit=True)
    implements(interfaces.IResourceController, inherit=True)
    implements(interfaces.IDomainObjectModification, inherit=True)
    implements(interfaces.IConfigurer)
    implements(interfaces.IConfigurable)
    implements(interfaces.IBlueprint, inherit=True)
    implements(IVersionedDatastoreQuerySchema)
    implements(interfaces.IClick)

    # IActions
    def get_actions(self):
        return create_actions(basic_search, crud, downloads, extras, multisearch)

    # IClick
    def get_commands(self):
        return cli.get_commands()

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
            u'datastore_ensure_privacy': auth.datastore_ensure_privacy,
            u'datastore_count': auth.datastore_count,
            u'datastore_multisearch': auth.datastore_multisearch,
            u'datastore_field_autocomplete': auth.datastore_field_autocomplete,
            u'datastore_create_slug': auth.datastore_create_slug,
            u'datastore_resolve_slug': auth.datastore_resolve_slug,
            u'datastore_queue_download': auth.datastore_queue_download,
            u'datastore_guess_fields': auth.datastore_guess_fields,
            u'datastore_hash_query': auth.datastore_hash_query,
            u'datastore_is_datastore_resource': auth.datastore_hash_query,
            u'datastore_get_latest_query_schema_version':
                auth.datastore_get_latest_query_schema_version,
        }

    # ITemplateHelpers
    def get_helpers(self):
        return {
            u'is_datastore_resource': is_datastore_resource,
            u'is_duplicate_ingestion': helpers.is_duplicate_ingestion,
            u'get_human_duration': helpers.get_human_duration,
            u'get_stat_icon': helpers.get_stat_icon,
            u'get_stat_activity_class': helpers.get_stat_activity_class,
            u'get_stat_title': helpers.get_stat_title,
            u'get_available_formats': helpers.get_available_formats,
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
            # correctly to its resource indexes
            update_resources_privacy(entity)
        elif isinstance(entity, model.Resource):
            context = {u'model': model, u'ignore_auth': True}
            data_dict = {u'resource_id': entity.id}

            if operation == DomainObjectOperation.deleted:
                toolkit.get_action(u'datastore_delete')(context, data_dict)
            else:
                do_upsert = False

                if operation == DomainObjectOperation.new:
                    # datastore_create returns True when the resource looks like it's ingestible
                    do_upsert = toolkit.get_action(u'datastore_create')(context, data_dict)
                elif operation == DomainObjectOperation.changed:
                    # always try the upsert if the resource has changed
                    do_upsert = True

                if do_upsert:
                    # in theory, last_modified should change when the resource file/url is changed
                    # and metadata_modified should change when any other attributes are changed. To
                    # cover off the possibility that this gets mixed up, we'll pick the max of them
                    modified = list(filter(None, (entity.last_modified, entity.metadata_modified)))
                    last_modified = max(modified) if modified else datetime.now()
                    data_dict[u'version'] = to_timestamp(last_modified)
                    # use replace to overwrite the existing data (this is what users would expect)
                    data_dict[u'replace'] = True
                    try:
                        toolkit.get_action(u'datastore_upsert')(context, data_dict)
                    except (ReadOnlyResourceException, InvalidVersionException):
                        # this is fine, just swallow
                        pass

    # IConfigurer
    def update_config(self, config):
        # add public folder containing schemas
        toolkit.add_public_directory(config, u'theme/public')
        # add templates
        toolkit.add_template_directory(config, u'theme/templates')
        toolkit.add_resource(u'theme/assets', u'ckanext-versioned-datastore')

    # IBlueprint
    def get_blueprint(self):
        return routes.blueprints

    # IConfigurable
    def configure(self, ckan_config):
        setup(ckan_config)

        # register all custom query schemas
        for plugin in PluginImplementations(IVersionedDatastoreQuerySchema):
            for version, schema in plugin.get_query_schemas():
                register_schema(version, schema)

        # reserve any requested slugs
        from .lib.query.slugs import reserve_slug
        for plugin in PluginImplementations(IVersionedDatastore):
            for reserved_pretty_slug, query_parameters in plugin.datastore_reserve_slugs().items():
                with suppress(Exception):
                    reserve_slug(reserved_pretty_slug, **query_parameters)

    # IVersionedDatastoreQuerySchema
    def get_query_schemas(self):
        return [
            (v1_0_0Schema.version, v1_0_0Schema())
        ]
