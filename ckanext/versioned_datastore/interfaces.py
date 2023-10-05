from ckan.plugins import interfaces


class IVersionedDatastore(interfaces.Interface):
    """
    Allow modifying versioned datastore logic.
    """

    def datastore_modify_data_dict(self, context, data_dict):
        """
        Allows modification of the data dict before it is validated and used to create
        the search object. This function should be used to remove/add/alter parameters
        in the data dict.

        :param context: the context
        :type context: dictionary
        :param data_dict: the parameters received from the user
        :type data_dict: dictionary
        """
        return data_dict

    def datastore_modify_search(self, context, original_data_dict, data_dict, search):
        """
        Allows modifications to the search before it is made. This is kind of analogous
        to IDatastore.datastore_search however instead of passing around a query dict,
        instead an elasticsearch-dsl Search object is passed.

        Each extension which implements this interface will be called in the order CKAN
        loaded them in, The search parameter will be the output of the previous
        extension's interface implementation, thus creating a chain of extensions, each
        getting a go at altering the search object if necessary. The base
        datastore_search function provides the initial search object.

        Implementors of this function should return the search object. Don't forget that
        most functions on the search object are chainable and create a copy of the
        search object - ensure you're returning the modified object!

        Two data dicts are passed into this function, the original data dict as it was
        before any ``datastore_modify_data_dict`` functions got to it, and the modified
        data_dict that was used to create the search object by the core functionality.
        This allows someone to, for example, remove a part of the data_dict in
        ``datastore_modify_data_dict`` to avoid it being added into the search object by
        the core functionality. Then, by implementing this function, they can add their
        custom search parts based on the details they removed by extracting them from
        the original_data_dict.

        :param context: the context
        :type context: dictionary
        :param original_data_dict: the parameters received from the user
        :type original_data_dict: dictionary
        :param data_dict: the parameters received from the user after they have been
                          modified by implementors of ``datastore_modify_data_dict``
        :type data_dict: dictionary
        :param search: the current search, as changed by the previous
                       IVersionedDatastore extensions in the chain
        :type search: elasticsearch-dsl Search object

        :returns: the search object with your modifications
        :rtype: elasticsearch-dsl Search object
        """
        return search

    def datastore_modify_result(self, context, original_data_dict, data_dict, result):
        """
        Allows modifications to the result after the search.

        Each extension which implements this interface will be called in the order CKAN
        loaded them in, The result parameter will be the output of the previous
        extension's interface implementation, thus creating a chain of extensions, each
        getting a go at altering the result object if necessary.

        Implementors of this function should return the result object so that the
        datastore_search function can build the final return dict.

        :param context: the context
        :type context: dictionary
        :param original_data_dict: the parameters received from the user
        :type original_data_dict: dictionary
        :param data_dict: the parameters received from the user after they have been
                          modified by implementors of ``datastore_modify_data_dict``
        :type data_dict: dictionary
        :param result: the current result, as changed by the previous
                       IVersionedDatastore extensions in the chain
        :type result: elasticsearch result object

        :returns: the result object with your modifications
        :rtype: elasticsearch result object
        """
        return result

    def datastore_modify_fields(self, resource_id, mapping, fields):
        """
        Allows modification of the field definitions before they are returned with the
        results of a datastore_search. The definitions are used in CKAN by the recline
        view and therefore need to abide by any of its requirements. By default all
        fields are included and are simply made up of a dict containing an id and type
        key. The id is the name of the field and the type is always string.

        :param resource_id: the resource id that was searched
        :param mapping: the mapping for the elasticsearch index containing the
                        resource's data. This is the raw mapping as a dict, retrieved
                        straight from elasticsearch's mapping endpoint
        :param fields: the field definitions that have so far been extracted from the
                       mapping, by default this is all fields
        :return: the list of field definition dicts
        """
        return fields

    def datastore_modify_index_doc(self, resource_id, index_doc):
        '''
        Action allowing the modification of a resource's data during indexing. The
        index_doc passed is a dict in the form:

            {
                "data": {},
                "meta": {}
            }

        which will be sent in this form to elasticsearch for indexing. The data key's
        value contains the data for the record at a version. The meta key's value
        contains metadata for the record so that we can search it correctly. Breakdown
        of the standard keys in the meta dict:

            - versions: a dict containing the range of versions this document is valid
                        for. This is represented using an elasticsearch range, with
                        "gte" for the  first valid version and "lt" for the last
                        version. If the "lt" key is missing the data is current.
            - version: the version of this record this data represents
            - next_version: will be missing if this data is current but if present, this
                            holds the value of the next version of this record

        If needed, the record id will be located in the index_doc under the key '_id'.

        :param resource_id: the id of the resource being indexed
        :param index_doc: a dict that will be sent to elasticsearch
        :return: the dict for elasticsearch to index
        '''
        return index_doc

    def datastore_is_read_only_resource(self, resource_id):
        """
        Allows implementors to designate certain resources as read only. This is purely
        a datastore side concept and should be used to prevent actions such as:

            - creating a new datastore for the resource (i.e. creating the index in
              elasticsearch)
            - upserting data into the datastore for this resource
            - deleting the datastore for this resource
            - reindexing data in the datastore for this resource

        :param resource_id: the resource id to check
        :return: True if the resource should be treated as read only, False if not
        """
        return False

    def datastore_after_indexing(self, request, splitgill_stats, stats_id):
        """
        Allows implementors to hook onto the completion of an indexing task. This
        function doesn't return anything and any exceptions it raises will be caught and
        ignored.

        :param request: the ResourceIndexRequest object that triggered the indexing task
        :param splitgill_stats: the statistics about the indexing task from splitgill
        :param stats_id: the id of the statistics entry in the ImportStats database
                         table
        """
        pass

    def datastore_reserve_slugs(self):
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
            - resource_ids_and_versions, a dict of resource ids and specific versions to
                                         search at (defaults to an empty dict)

        If a slug already exists in the database with the same reserved pretty slug and
        the same query parameters then nothing happens.

        If a slug already exists in the database with the same reserved pretty slug but
        a different set of query parameters then a DuplicateSlugException is raised.

        If a slug already exists in the database with the same query parameters but no
        reserved pretty slug then the reserved pretty slug is added to the slug.
        """
        return {}

    def datastore_modify_guess_fields(self, resource_ids, fields):
        """
        Allows plugins to manipulate the Fields object used to figure out the groups
        that should be returned by the datastore_guess_fields action.

        :param resource_ids: a list of resource ids
        :param fields: a Fields object
        :return: the Fields object
        """
        return fields

    def datastore_multisearch_modify_response(self, response):
        """
        Allows plugins to alter the response dict returned from the
        datastore_multisearch action before it is returned.

        :param response: the dict to be returned to the caller
        :return: the response dict
        """
        return response

    def datastore_before_convert_basic_query(self, basic_query):
        """
        Allows plugins to modify a basic query (probably taken from a URL), e.g. to
        remove custom filters before processing.

        :param basic_query: the query dict to be modified
        :return: the modified query
        """
        return basic_query

    def datastore_after_convert_basic_query(self, basic_query, multisearch_query):
        """
        Allows plugins to modify a converted query, e.g. to add back in any complex
        custom filters.

        :param basic_query: the original basic query, before it was modified by other
                            plugins
        :param multisearch_query: the converted multisearch version of the query
        :return: the modified multisearch query
        """
        return multisearch_query


class IVersionedDatastoreQuerySchema(interfaces.Interface):
    def get_query_schemas(self):
        """
        Hook to allow registering custom query schemas.

        :return: a list of tuples of the format (query schema version, schema object)
                 where the query schema version is a string of format v#.#.# and the
                 schema object is an instance of
                 ckanext.versioned_datastore.lib.query.Schema
        """
        return []


class IVersionedDatastoreDownloads(interfaces.Interface):
    def download_modify_notifier_start_templates(
        self, text_template: str, html_template: str
    ):
        """
        Hook allowing other extensions to modify the templates used when sending
        notifications that a download has started. The templates can be modified in
        place or completely replaced.

        :param text_template: the text template string
        :param html_template: the html template string
        :return: a 2-tuple containing the text template string and the html template
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
        :return: a 2-tuple containing the text template string and the html template
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
        :return: a 2-tuple containing the text template string and the html template
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
        :return: context templating dict
        """
        return context

    def download_derivative_generators(self, registered_derivatives=None):
        """
        Extend or modify the list of derivative generators.

        :param registered_derivatives: a dict of existing derivative generator classes,
                                       returned from previously loaded plugins
        :return: a dict of loaded derivative generator classes, keyed on the name used
                 to specify them in download requests
        """
        return registered_derivatives or {}

    def download_file_servers(self, registered_servers=None):
        """
        Extend or modify the list of file servers.

        :param registered_servers: a dict of existing file server classes, returned from
                                   previously loaded plugins
        :return: a dict of loaded file server classes, keyed on the name used to specify
                 them in download requests
        """
        return registered_servers or {}

    def download_notifiers(self, registered_notifiers=None):
        """
        Extend or modify the list of download notifiers.

        :param registered_notifiers: a dict of existing notifier classes, returned from
                                     previously loaded plugins
        :return: a dict of loaded notifier classes, keyed on the name used to specify
                 them in download requests
        """
        return registered_notifiers or {}

    def download_data_transformations(self, registered_transformations=None):
        """
        Extend or modify the list of data transformations.

        :param registered_transformations: a dict of existing data transformations,
                                           returned from previously loaded plugins
        :return: a dict of loaded transformations, keyed on the name used to specify
                 them in download requests
        """
        return registered_transformations or {}

    def download_before_run(
        self, query_args, derivative_args, server_args, notifier_args
    ):
        """
        Hook allowing other extensions to modify args before any search is run or files
        generated.
        FIXME: this should be renamed to download_before_init or similar

        :param query_args: a QueryArgs object
        :param derivative_args: a DerivativeArgs object
        :param server_args: a ServerArgs object
        :param notifier_args: a NotifierArgs object
        :return: all four args objects (query_args, derivative_args, server_args,
                 notifier_args)
        """
        return query_args, derivative_args, server_args, notifier_args

    def download_after_init(self, query):
        """
        Hook notifying that the downloader and associated objects (e.g. the query) have
        been initialised. Does not allow modification; purely for notification purposes.

        :param query: the query for this download
        :return: None
        """
        return

    def download_modify_manifest(self, manifest, request):
        """
        Hook allowing other extensions to modify the manifest before the download file
        is written. Modifications to the request object are not saved.

        :param manifest: the manifest dict
        :param request: the DownloadRequest object
        :return: the manifest dict
        """
        return manifest

    def download_modify_eml(self, eml_dict, query):
        """
        Hook allowing other extensions to modify the content of the EML before it's
        transformed into XML and written to file.

        :param eml_dict: the current eml content, as a dict
        :param query: the query for this download
        :return: the modified eml dict
        """
        return eml_dict

    def download_after_run(self, request):
        """
        Hook notifying that a download has finished (whether failed or completed). Does
        not allow modification; purely for notification purposes.

        :param request: the DownloadRequest object
        :return: None
        """
        return
