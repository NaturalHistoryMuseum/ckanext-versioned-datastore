import logging
from contextlib import suppress
from typing import List, Optional

from ckan.plugins import (
    SingletonPlugin,
    implements,
    interfaces,
    toolkit,
)
from ckantools.loaders import create_actions, create_auth
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from splitgill.manager import SplitgillClient

from ckanext.versioned_datastore import cli, helpers, routes
from ckanext.versioned_datastore.interfaces import IVersionedDatastoreQuerySchema
from ckanext.versioned_datastore.lib.query.schema import register_schema
from ckanext.versioned_datastore.lib.query.schemas.v1_0_0 import v1_0_0Schema
from ckanext.versioned_datastore.lib.query.search.query import SchemaQuery
from ckanext.versioned_datastore.lib.tasks import get_es_health, get_queue_length
from ckanext.versioned_datastore.lib.utils import (
    RawResourceException,
    ReadOnlyResourceException,
    iqs_implementations,
    is_datastore_resource,
    ivds_implementations,
)
from ckanext.versioned_datastore.logic.basic import (
    action as basic_action,
)
from ckanext.versioned_datastore.logic.basic import (
    auth as basic_auth,
)
from ckanext.versioned_datastore.logic.data import (
    action as data_action,
)
from ckanext.versioned_datastore.logic.data import (
    auth as data_auth,
)
from ckanext.versioned_datastore.logic.download import (
    action as download_action,
)
from ckanext.versioned_datastore.logic.download import (
    auth as download_auth,
)
from ckanext.versioned_datastore.logic.multi import (
    action as multi_action,
)
from ckanext.versioned_datastore.logic.multi import (
    auth as multi_auth,
)
from ckanext.versioned_datastore.logic.options import (
    action as options_action,
)
from ckanext.versioned_datastore.logic.options import (
    auth as options_auth,
)
from ckanext.versioned_datastore.logic.resource import (
    action as resource_action,
)
from ckanext.versioned_datastore.logic.resource import (
    auth as resource_auth,
)
from ckanext.versioned_datastore.logic.schema import (
    action as schema_action,
)
from ckanext.versioned_datastore.logic.schema import (
    auth as schema_auth,
)
from ckanext.versioned_datastore.logic.slug import (
    action as slug_action,
)
from ckanext.versioned_datastore.logic.slug import (
    auth as slug_auth,
)
from ckanext.versioned_datastore.logic.version import (
    action as version_action,
)
from ckanext.versioned_datastore.logic.version import (
    auth as version_auth,
)

try:
    from ckanext.status.interfaces import IStatus

    status_available = True
except ImportError:
    status_available = False

log = logging.getLogger(__name__)

# stop elasticsearch from showing warning logs
logging.getLogger('elasticsearch').setLevel(logging.ERROR)


class VersionedSearchPlugin(SingletonPlugin):
    implements(interfaces.IActions)
    implements(interfaces.IAuthFunctions)
    implements(interfaces.ITemplateHelpers, inherit=True)
    implements(interfaces.IResourceController, inherit=True)
    implements(interfaces.IConfigurer)
    implements(interfaces.IConfigurable)
    implements(interfaces.IBlueprint, inherit=True)
    implements(IVersionedDatastoreQuerySchema)
    implements(interfaces.IClick)
    if status_available:
        implements(IStatus)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # all these are set up during the configure method
        self.mongo_client: Optional[MongoClient] = None
        self.elasticsearch_client: Optional[Elasticsearch] = None
        self.sg_client: Optional[SplitgillClient] = None

    # IConfigurable
    def configure(self, ckan_config):
        # do the setup for Splitgill first, create Mongo client, Elasticsearch client,
        # and then the Splitgill client
        self.mongo_client = MongoClient(
            host=ckan_config.get('ckanext.versioned_datastore.mongo_host'),
            port=int(ckan_config.get('ckanext.versioned_datastore.mongo_port')),
        )
        es_hosts = ckan_config.get(
            'ckanext.versioned_datastore.elasticsearch_hosts'
        ).split(',')
        es_port = ckan_config.get('ckanext.versioned_datastore.elasticsearch_port')
        self.elasticsearch_client = Elasticsearch(
            hosts=[f'http://{host}:{es_port}/' for host in es_hosts],
            # todo: check these params
            sniff_on_start=True,
            sniff_on_node_failure=True,
            sniff_timeout=30,
            http_compress=False,
            request_timeout=60,
        )
        mongo_db_name = ckan_config.get('ckanext.versioned_datastore.mongo_database')
        self.sg_client = SplitgillClient(
            self.mongo_client, self.elasticsearch_client, mongo_db_name
        )

        # register all custom query schemas
        for plugin in iqs_implementations():
            for query_version, query_schema in plugin.get_query_schemas():
                register_schema(query_version, query_schema)

        # reserve any requested slugs
        from .lib.query.slugs.slugs import reserve_slug

        for plugin in ivds_implementations():
            slugs = plugin.vds_reserve_slugs()
            for reserved_pretty_slug, query_parameters in slugs.items():
                query = SchemaQuery(**query_parameters)
                with suppress(Exception):
                    reserve_slug(reserved_pretty_slug, query)

    def is_sg_configured(self) -> bool:
        """
        Returns whether Splitgill is configured and ready for use. This checks if the
        Mongo client, Elasticsearch client, and Splitgill client have all been created
        and stored against this plugin instance.

        :returns: True if it's ready, False if not
        """
        return (
            self.mongo_client is not None
            and self.elasticsearch_client is not None
            and self.sg_client is not None
        )

    # IActions
    def get_actions(self):
        return create_actions(
            basic_action,
            data_action,
            download_action,
            multi_action,
            options_action,
            resource_action,
            schema_action,
            slug_action,
            version_action,
        )

    # IAuthFunctions
    def get_auth_functions(self):
        return create_auth(
            basic_auth,
            data_auth,
            download_auth,
            multi_auth,
            options_auth,
            resource_auth,
            schema_auth,
            slug_auth,
            version_auth,
        )

    # IClick
    def get_commands(self):
        return cli.get_commands()

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
        # ensure datastore_active is set where it should be
        resource_dict['datastore_active'] = is_datastore_resource(resource_dict['id'])
        # theoretically a resource could be datastore_active and have parsing disabled
        # at the same time if the database and ES have gotten out of sync, which isn't
        # ideal, but the fixes are more annoying than the problem itself
        return resource_dict

    # IResourceController
    def before_create(self, context, resource):
        # only set disable_parsing if it's True (False by default)
        if toolkit.asbool(resource.pop('disable_parsing', False)):
            resource['disable_parsing'] = True

    # IResourceController
    def before_update(self, context, current, resource):
        # we can't automatically go from ingested to raw because it might be included
        # in queries and DOIs, but we can ingest a file that was previously raw
        was_raw = toolkit.asbool(current.get('disable_parsing', False))
        is_raw = toolkit.asbool(resource.pop('disable_parsing', False))
        if was_raw and is_raw:
            # only allow if it was already previously raw
            resource['disable_parsing'] = True

    # IResourceController
    def after_update(self, context: dict, resource: dict):
        # use replace to overwrite the existing data (this is what users would expect)
        data_dict = {'resource_id': resource['id'], 'replace': True}
        with suppress(ReadOnlyResourceException), suppress(RawResourceException):
            toolkit.get_action('vds_data_add')(context, data_dict)

    # IResourceController
    def after_create(self, context: dict, resource: dict):
        # use replace to overwrite the existing data (this is what users would expect)
        data_dict = {'resource_id': resource['id'], 'replace': True}
        with suppress(ReadOnlyResourceException), suppress(RawResourceException):
            toolkit.get_action('vds_data_add')(context, data_dict)

    def before_delete(self, context: dict, resource: dict, resources: List[dict]):
        toolkit.get_action('vds_data_delete')(context, {'resource_id': resource['id']})

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
                'help': toolkit._('Number of downloads waiting in the queue'),
                'state': 'good'
                if queued_downloads < 2
                else ('ok' if queued_downloads < 4 else 'bad'),
            }
        )

        queued_imports = get_queue_length('importing')

        status_reports.append(
            {
                'label': toolkit._('Imports'),
                'value': queued_imports,
                'group': toolkit._('Queues'),
                'help': toolkit._('Number of import jobs waiting in the queue'),
                'state': 'good'
                if queued_imports < 2
                else ('ok' if queued_imports < 4 else 'bad'),
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
