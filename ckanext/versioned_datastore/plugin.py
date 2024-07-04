import logging
from contextlib import suppress
from datetime import datetime

from ckan import model
from ckan.model import DomainObjectOperation
from ckan.plugins import (
    toolkit,
    interfaces,
    SingletonPlugin,
    implements,
    PluginImplementations,
)
from splitgill.utils import to_timestamp

from . import routes, helpers, cli
from .interfaces import IVersionedDatastoreQuerySchema, IVersionedDatastore
from .lib.common import setup
from .lib.datastore_utils import (
    is_datastore_resource,
    ReadOnlyResourceException,
    InvalidVersionException,
    update_resources_privacy,
    get_queue_length,
    get_es_health,
)
from .lib.query.schema import register_schema
from .lib.query.v1_0_0 import v1_0_0Schema
from .logic import auth
from .logic.actions import basic_search, crud, downloads, extras, multisearch
from ckantools.loaders import create_actions, create_auth

try:
    from ckanext.status.interfaces import IStatus

    status_available = True
except ImportError:
    status_available = False

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
    if status_available:
        implements(IStatus)

    # IActions
    def get_actions(self):
        return create_actions(basic_search, crud, downloads, extras, multisearch)

    # IClick
    def get_commands(self):
        return cli.get_commands()

    # IAuthFunctions
    def get_auth_functions(self):
        return create_auth(auth)

    # ITemplateHelpers
    def get_helpers(self):
        return {
            'is_datastore_resource': is_datastore_resource,
            'is_duplicate_ingestion': helpers.is_duplicate_ingestion,
            'get_human_duration': helpers.get_human_duration,
            'get_stat_icon': helpers.get_stat_icon,
            'get_stat_activity_class': helpers.get_stat_activity_class,
            'get_stat_title': helpers.get_stat_title,
            'get_available_formats': helpers.get_available_formats,
            'pretty_print_json': helpers.pretty_print_json,
            'get_version_date': helpers.get_version_date,
            'latest_item_version': helpers.latest_item_version,
            'nav_slug': helpers.nav_slug,
        }

    # IResourceController
    def before_show(self, resource_dict):
        resource_dict['datastore_active'] = is_datastore_resource(resource_dict['id'])
        return resource_dict

    # IDomainObjectModification
    def notify(self, entity, operation):
        """
        Respond to changes to model objects. We use this hook to ensure any new data is
        imported into the versioned datastore and to make sure the privacy settings on
        the data are up to date. We're only interested in:

            - resource deletions
            - new resources
            - resources that have had changes to their URL
            - packages that have changed

        :param entity: the entity that has changed
        :param operation: the operation undertaken on the object. This will be one of the options
                          from the DomainObjectOperation enum.
        """
        if (
            isinstance(entity, model.Package)
            and operation == DomainObjectOperation.changed
        ):
            # if a package is the target entity and it's been changed ensure the privacy is applied
            # correctly to its resource indexes
            update_resources_privacy(entity)
        elif isinstance(entity, model.Resource):
            context = {'model': model, 'ignore_auth': True}
            data_dict = {'resource_id': entity.id}

            if operation == DomainObjectOperation.deleted:
                toolkit.get_action('datastore_delete')(context, data_dict)
            else:
                do_upsert = False

                if operation == DomainObjectOperation.new:
                    # datastore_create returns True when the resource looks like it's ingestible
                    do_upsert = toolkit.get_action('datastore_create')(
                        context, data_dict
                    )
                elif operation == DomainObjectOperation.changed:
                    # always try the upsert if the resource has changed
                    do_upsert = True

                if do_upsert:
                    # in theory, last_modified should change when the resource file/url is changed
                    # and metadata_modified should change when any other attributes are changed. To
                    # cover off the possibility that this gets mixed up, we'll pick the max of them
                    modified = list(
                        filter(None, (entity.last_modified, entity.metadata_modified))
                    )
                    last_modified = max(modified) if modified else datetime.now()
                    data_dict['version'] = to_timestamp(last_modified)
                    # use replace to overwrite the existing data (this is what users would expect)
                    data_dict['replace'] = True
                    # these exceptions are fine to swallow
                    with suppress(ReadOnlyResourceException, InvalidVersionException):
                        toolkit.get_action('datastore_upsert')(context, data_dict)

    # IConfigurer
    def update_config(self, config):
        # add public folder containing schemas
        toolkit.add_public_directory(config, 'theme/public')
        # add templates
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/assets', 'ckanext-versioned-datastore')

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
            for (
                reserved_pretty_slug,
                query_parameters,
            ) in plugin.datastore_reserve_slugs().items():
                with suppress(Exception):
                    reserve_slug(reserved_pretty_slug, **query_parameters)

    # IVersionedDatastoreQuerySchema
    def get_query_schemas(self):
        return [(v1_0_0Schema.version, v1_0_0Schema())]

    # IStatus
    def modify_status_reports(self, status_reports):
        queued_downloads = get_queue_length('download')

        status_reports.append(
            {
                'label': toolkit._('Downloads'),
                'value': queued_downloads,
                'group': toolkit._('Queues'),
                'help': toolkit._(
                    'Number of downloads either currently processing or waiting in the queue'
                ),
                'state': 'good'
                if queued_downloads == 0
                else ('ok' if queued_downloads < 3 else 'bad'),
            }
        )

        queued_imports = get_queue_length('importing')

        status_reports.append(
            {
                'label': toolkit._('Imports'),
                'value': queued_imports,
                'group': toolkit._('Queues'),
                'help': toolkit._(
                    'Number of import jobs either currently processing or waiting in the queue'
                ),
                'state': 'good'
                if queued_imports == 0
                else ('ok' if queued_imports < 3 else 'bad'),
            }
        )

        es_health = get_es_health()
        server_status_text = (
            toolkit._('available') if es_health['ping'] else toolkit._('unavailable')
        )

        status_reports.append(
            {
                'label': toolkit._('Search'),
                'value': server_status_text,
                'help': toolkit._(
                    'Multisearch functionality is provided by an Elasticsearch cluster'
                ),
                'state': 'good' if es_health['ping'] else 'bad',
            }
        )

        return status_reports
