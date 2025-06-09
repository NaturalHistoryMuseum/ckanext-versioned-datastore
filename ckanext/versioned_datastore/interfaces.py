from ckan.plugins.interfaces import Interface
from splitgill.indexing.options import ParsingOptionsBuilder


class IVersionedDatastore(Interface):
    """
    Allow modifying versioned datastore logic.
    """

    def vds_before_search(self, request):
        """
        Called right before the Search object is created and performed. Implementing
        this hook allows modification of the request (including the query etc) before it
        is run. It doesn't matter how the search request is made, this hook will be
        called, so make sure your implementation works with all Query types etc.

        :param request: a SearchRequest instance
        """
        pass

    def vds_after_multi_query(self, response, result):
        pass

    def vds_is_read_only_resource(self, resource_id: str):
        """
        Allows implementors to designate certain resources as read only. This is purely
        a datastore concept and doesn't impact the actual resource from CKAN's point of
        view. This is checked when performing the following actions:

            - vds_data_add
            - vds_data_delete
            - vds_data_sync

        :param resource_id: the resource id to check
        :returns: True if the resource should be treated as read only, False if not
        """
        return False

    def vds_update_options(self, resource_id: str, builder: ParsingOptionsBuilder):
        pass

    def vds_reserve_slugs(self):
        """
        Allows implementors to reserve queries using reserved pretty slugs. Implementors
        should return a dict made up of reserved pretty slugs as keys and then the slug
        parameters as the values. These values should be another dict containing the
        following optional keys:

            - query, a dict query (defaults to {})
            - query_version, the query schema version (defaults to the latest query
                             schema version)
            - version, the version of the data to search at (defaults to None)
            - resource_ids, a list of resource ids to search (defaults to all resource
                            ids)

        If a slug already exists in the database with the same reserved pretty slug and
        the same query parameters then nothing happens.

        If a slug already exists in the database with the same reserved pretty slug but
        a different set of query parameters then a DuplicateSlugException is raised.

        If a slug already exists in the database with the same query parameters but no
        reserved pretty slug then the reserved pretty slug is added to the slug.
        """
        return {}

    def vds_modify_field_groups(self, resource_ids, fields):
        """
        Allows plugins to manipulate the FieldGroups object used to figure out the
        groups that should be returned by the vds_multi_fields action.

        :param resource_ids: a list of resource ids
        :param fields: a FieldGroups object
        """
        pass

    def datastore_before_convert_basic_query(self, basic_query):
        """
        Allows plugins to modify a basic query (probably taken from a URL), e.g. to
        remove custom filters before processing.

        :param basic_query: the query dict to be modified
        :returns: the modified query
        """
        return basic_query

    def datastore_after_convert_basic_query(self, basic_query, multisearch_query):
        """
        Allows plugins to modify a converted query, e.g. to add back in any complex
        custom filters.

        :param basic_query: the original basic query, before it was modified by other
            plugins
        :param multisearch_query: the converted multisearch version of the query
        :returns: the modified multisearch query
        """
        return multisearch_query


class IVersionedDatastoreQuerySchema(Interface):
    def get_query_schemas(self):
        """
        Hook to allow registering custom query schemas.

        :returns: a list of tuples of the format (query schema version, schema object)
            where the query schema version is a string of format v#.#.# and the schema
            object is an instance of ckanext.versioned_datastore.lib.query.Schema
        """
        return []


class IVersionedDatastoreDownloads(Interface):
    def download_modify_notifier_start_templates(
        self, text_template: str, html_template: str
    ):
        """
        Hook allowing other extensions to modify the templates used when sending
        notifications that a download has started. The templates can be modified in
        place or completely replaced.

        :param text_template: the text template string
        :param html_template: the html template string
        :returns: a 2-tuple containing the text template string and the html template
            string
        """
        return text_template, html_template

    def download_modify_notifier_end_templates(
        self, text_template: str, html_template: str
    ):
        """
        Hook allowing other extensions to modify the templates used when sending
        notifications that a download has completed successfully. The templates can be
        modified in place or completely replaced.

        :param text_template: the text template string
        :param html_template: the html template string
        :returns: a 2-tuple containing the text template string and the html template
            string
        """
        return text_template, html_template

    def download_modify_notifier_error_templates(
        self, text_template: str, html_template: str
    ):
        """
        Hook allowing other extensions to modify the templates used when sending
        notifications that a download has failed. The templates can be modified in place
        or completely replaced.

        :param text_template: the text template string
        :param html_template: the html template string
        :returns: a 2-tuple containing the text template string and the html template
            string
        """
        return text_template, html_template

    def download_modify_notifier_template_context(self, request, context):
        """
        Hook allowing other extensions to modify the templating context used to generate
        the download email (both plain text and HTML versions) before it is sent.

        The default context contains:
            - "download_url": the download zip's full URL
            - "site_url": the CKAN site's full URL (this is taken straight from the
                          config)

        :param request: the DownloadRequest object
        :param context: templating context dict
        :returns: context templating dict
        """
        return context

    def download_derivative_generators(self, registered_derivatives=None):
        """
        Extend or modify the list of derivative generators.

        :param registered_derivatives: a dict of existing derivative generator classes,
            returned from previously loaded plugins
        :returns: a dict of loaded derivative generator classes, keyed on the name used
            to specify them in download requests
        """
        return registered_derivatives or {}

    def download_file_servers(self, registered_servers=None):
        """
        Extend or modify the list of file servers.

        :param registered_servers: a dict of existing file server classes, returned from
            previously loaded plugins
        :returns: a dict of loaded file server classes, keyed on the name used to
            specify them in download requests
        """
        return registered_servers or {}

    def download_notifiers(self, registered_notifiers=None):
        """
        Extend or modify the list of download notifiers.

        :param registered_notifiers: a dict of existing notifier classes, returned from
            previously loaded plugins
        :returns: a dict of loaded notifier classes, keyed on the name used to specify
            them in download requests
        """
        return registered_notifiers or {}

    def download_data_transformations(self, registered_transformations=None):
        """
        Extend or modify the list of data transformations.

        :param registered_transformations: a dict of existing data transformations,
            returned from previously loaded plugins
        :returns: a dict of loaded transformations, keyed on the name used to specify
            them in download requests
        """
        return registered_transformations or {}

    def download_before_init(
        self, query_args, derivative_args, server_args, notifier_args
    ):
        """
        Hook allowing other extensions to modify args before any search is run or files
        generated.

        :param query_args: a QueryArgs object
        :param derivative_args: a DerivativeArgs object
        :param server_args: a ServerArgs object
        :param notifier_args: a NotifierArgs object
        :returns: all four args objects (query_args, derivative_args, server_args,
            notifier_args)
        """
        return query_args, derivative_args, server_args, notifier_args

    def download_after_init(self, query):
        """
        Hook notifying that the downloader and associated objects (e.g. the query) have
        been initialised. Does not allow modification; purely for notification purposes.

        :param query: the query for this download
        :returns: None
        """
        return

    def download_modify_manifest(self, manifest, request):
        """
        Hook allowing other extensions to modify the manifest before the download file
        is written. Modifications to the request object are not saved.

        :param manifest: the manifest dict
        :param request: the DownloadRequest object
        :returns: the manifest dict
        """
        return manifest

    def download_modify_eml(self, eml_dict, query):
        """
        Hook allowing other extensions to modify the content of the EML before it's
        transformed into XML and written to file.

        :param eml_dict: the current eml content, as a dict
        :param query: the query for this download
        :returns: the modified eml dict
        """
        return eml_dict

    def download_after_run(self, request):
        """
        Hook notifying that a download has finished (whether failed or completed). Does
        not allow modification; purely for notification purposes.

        :param request: the DownloadRequest object
        :returns: None
        """
        return
