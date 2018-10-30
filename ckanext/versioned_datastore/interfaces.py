import ckan.plugins.interfaces as interfaces


class IVersionedDatastore(interfaces.Interface):
    '''
    Allow modifying versioned datastore queries.
    '''

    def datastore_modify_data_dict(self, context, data_dict):
        '''
        Allows modification of the data dict before it is validated and used to create the search
        object. This function should be used to remove/add/alter parameters in the data dict.

        :param context: the context
        :type context: dictionary
        :param data_dict: the parameters received from the user
        :type data_dict: dictionary
        '''
        return data_dict

    def datastore_modify_search(self, context, original_data_dict, data_dict, search):
        '''
        Allows modifications to the search before it is made. This is kind of analogous to
        IDatastore.datastore_search however instead of passing around a query dict, instead an
        elasticsearch-dsl Search object is passed.

        Each extension which implements this interface will be called in the order CKAN loaded them
        in, The search parameter will be the output of the previous extension's interface
        implementation, thus creating a chain of extensions, each getting a go at altering the
        search object if necessary. The base datastore_search function provides the initial search
        object.

        Implementors of this function should return the search object. Don't forget that most
        functions on the search object are chainable and create a copy of the search object - ensure
        you're returning the modified object!

        Two data dicts are passed into this function, the original data dict as it was before any
        ``datastore_modify_data_dict`` functions got to it, and the modified data_dict that was used
        to create the search object by the core functionality. This allows someone to, for example,
        remove a part of the data_dict in ``datastore_modify_data_dict`` to avoid it being added
        into the search object by the core functionality. Then, by implementing this function,
        they can add their custom search parts based on the details they removed by extracting them
        from the original_data_dict.

        :param context: the context
        :type context: dictionary
        :param original_data_dict: the parameters received from the user
        :type original_data_dict: dictionary
        :param data_dict: the parameters received from the user after they have been modified by
                          implementors of ``datastore_modify_data_dict``
        :type data_dict: dictionary
        :param search: the current search, as changed by the previous IVersionedDatastore extensions
                       in the chain
        :type search: elasticsearch-dsl Search object

        :returns: the search object with your modifications
        :rtype: elasticsearch-dsl Search object
        '''
        return search

    def datastore_modify_result(self, context, original_data_dict, data_dict, result):
        '''
        Allows modifications to the result after the search.

        Each extension which implements this interface will be called in the order CKAN loaded them
        in, The search parameter will be the output of the previous extension's interface
        implementation, thus creating a chain of extensions, each getting a go at altering the
        search object if necessary. The base datastore_search function provides the initial result
        object.

        Implementors of this function should return the result object so that the datastore_search
        function can build the final return dict.

        :param context: the context
        :type context: dictionary
        :param original_data_dict: the parameters received from the user
        :type original_data_dict: dictionary
        :param data_dict: the parameters received from the user after they have been modified by
                          implementors of ``datastore_modify_data_dict``
        :type data_dict: dictionary
        :param result: the current result, as changed by the previous IVersionedDatastore extensions
                       in the chain
        :type result: eevee SearchResults object

        :returns: the result object with your modifications
        :rtype: eevee SearchResults object
        '''
        return result

    def datastore_modify_fields(self, resource_id, mapping, fields):
        '''
        Allows modification of the field definitions before they are returned with the results of
        a datastore_search. The definitions are used in CKAN by the recline view and therefore need
        to abide by any of its requirements. By default all fields are included and are simply made
        up of a dict containing an id and type key. The id is the name of the field and the type is
        always string.

        :param resource_id: the resource id that was searched
        :param mapping: the mapping for the elasticsearch index containing the resource's data. This
                        is the raw mapping as a dict, retrieved straight from elasticsearch's
                        mapping endpoint
        :param fields: the field definitions that have so far been extracted from the mapping, by
                       default this is all fields
        :return: the list of field definition dicts
        '''
        return fields

    def datastore_modify_index_doc(self, resource_id, index_doc):
        '''
        Action allowing the modification of a resource's data during indexing. The index_doc passed
        is a dict in the form:

            {
                "data": {},
                "meta": {}
            }

        which will be sent in this form to elasticsearch for indexing. The data key's value contains
        the data for the record at a version. The meta key's value contains metadata for the record
        so that we can search it correctly. Breakdown of the standard keys in the meta dict:

            - versions: a dict containing the range of versions this document is valid for. This is
                        represented using an elasticsearch range, with "gte" for the first valid
                        version and "lt" for the last version. If the "lt" key is missing the data
                        is current.
            - version: the version of this record this data represents
            - next_version: will be missing if this data is current but if present, this holds the
                            value of the next version of this record

        If needed, the record id will be located in the index_doc under the key '_id'.

        :param resource_id: the id of the resource being indexed
        :param index_doc: a dict that will be sent to elasticsearch
        :return: the dict for elasticsearch to index
        '''
        return index_doc
