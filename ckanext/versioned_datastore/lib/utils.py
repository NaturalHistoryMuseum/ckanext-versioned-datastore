from ckan import model
from ckan.lib.search import SearchIndexError
from ckan.plugins import toolkit, PluginImplementations
from ckanext.versioned_datastore.interfaces import IVersionedDatastore
from ckanext.versioned_datastore.lib.details import get_all_details
from eevee.config import Config
from eevee.indexing.utils import DOC_TYPE
from eevee.search import SearchHelper, create_version_query
from elasticsearch import NotFoundError
from elasticsearch_dsl import Search, MultiSearch

# if the resource has been side loaded into the datastore then this should be its URL
DATASTORE_ONLY_RESOURCE = u'_datastore_only_resource'
# the formats we support for ingestion
CSV_FORMATS = [u'csv', u'application/csv']
TSV_FORMATS = [u'tsv']
XLS_FORMATS = [u'xls', u'application/vnd.ms-excel']
XLSX_FORMATS = [u'xlsx', u'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
ALL_FORMATS = CSV_FORMATS + TSV_FORMATS + XLS_FORMATS + XLSX_FORMATS

# global variables to hold the eevee config (not the CKAN one), the eevee search helper object and
# an elasticsearch client object
CONFIG = None
SEARCH_HELPER = None
CLIENT = None


def setup_eevee(ckan_config):
    '''
    Given the CKAN config, create the Eevee config object and the eevee Searcher object.

    :param ckan_config: the ckan config
    '''
    global CONFIG
    global SEARCH_HELPER
    global CLIENT

    es_hosts = ckan_config.get(u'ckanext.versioned_datastore.elasticsearch_hosts').split(u',')
    es_port = ckan_config.get(u'ckanext.versioned_datastore.elasticsearch_port')
    prefix = ckan_config.get(u'ckanext.versioned_datastore.elasticsearch_index_prefix')
    CONFIG = Config(
        elasticsearch_hosts=[u'http://{}:{}/'.format(host, es_port) for host in es_hosts],
        elasticsearch_index_prefix=prefix,
        mongo_host=ckan_config.get(u'ckanext.versioned_datastore.mongo_host'),
        mongo_port=int(ckan_config.get(u'ckanext.versioned_datastore.mongo_port')),
        mongo_database=ckan_config.get(u'ckanext.versioned_datastore.mongo_database'),
    )
    SEARCH_HELPER = SearchHelper(CONFIG)
    # for convenience, expose the client in the search helper at the module level
    CLIENT = SEARCH_HELPER.client


def get_latest_version(resource_id):
    '''
    Retrieves the latest version of the given resource from the status index.

    :param resource_id: the resource's id
    :return: the version or None if the resource isn't indexed
    '''
    index_name = prefix_resource(resource_id)
    return SEARCH_HELPER.get_latest_index_versions([index_name]).get(index_name, None)


def validate(context, data_dict, default_schema):
    '''
    Validate the data_dict against a schema. If a schema is not available in the context (under the
    key 'schema') then the default schema is used.

    If the data_dict fails the validation process a ValidationError is raised, otherwise the
    potentially updated data_dict is returned.

    :param context: the ckan context dict
    :param data_dict: the dict to validate
    :param default_schema: the default schema to use if the context doesn't have one
    '''
    schema = context.get(u'schema', default_schema)
    data_dict, errors = toolkit.navl_validate(data_dict, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)
    return data_dict


def prefix_resource(resource_id):
    '''
    Adds the configured prefix to the start of the resource id to get the index name for the
    resource data in elasticsearch.

    :param resource_id: the resource id
    :return: the resource's index name
    '''
    return u'{}{}'.format(CONFIG.elasticsearch_index_prefix, resource_id)


def unprefix_index(index_name):
    '''
    Removes the configured prefix from the start of the index name to get the resource id.

    :param index_name: the index name
    :return: the resource's id
    '''
    return index_name[len(CONFIG.elasticsearch_index_prefix):]


def prefix_field(field):
    '''
    Prefixes a the given field name with "data.". All data from the resource in eevee is stored
    under the data key in the elasticsearch record so to avoid end users needing to know that all
    fields should be referenced by their non-data.-prefixed name until they are internal to the code
    and can be prefixed before being passed on to eevee.

    :param field: the field name
    :return: data.<field>
    '''
    return u'data.{}'.format(field)


def format_facets(aggs):
    '''
    Formats the facet aggregation result into the format we require. Specifically we expand the
    buckets out into a dict that looks like this:

        {
            "facet1": {
                "details": {
                    "sum_other_doc_count": 34,
                    "doc_count_error_upper_bound": 3
                },
                "values": {
                    "value1": 1,
                    "value2": 4,
                    "value3": 1,
                    "value4": 2,
                }
            },
            etc
        }

    etc.

    :param aggs: the aggregation dict returned from eevee/elasticsearch
    :return: the facet information as a dict
    '''
    facets = {}
    for facet, details in aggs.items():
        facets[facet] = {
            u'details': {
                u'sum_other_doc_count': details[u'sum_other_doc_count'],
                u'doc_count_error_upper_bound': details[u'doc_count_error_upper_bound'],
            },
            u'values': {value_details[u'key']: value_details[u'doc_count']
                        for value_details in details[u'buckets']}
        }

    return facets


# this dict stores cached get_field returns. It is only cleared by restarting the server. This is
# safe because the cached data is keyed on the rounded version and is therefore stable as old
# versions of data can't be modified, so the fields will always be valid. If for some reason this
# isn't the case (such as if redactions for specific fields get added later and old versions of
# records are updated) then the server just needs a restart and that's it).
field_cache = {}


def get_fields(resource_id, version=None):
    '''
    Given a resource id, returns the fields that existed at the given version. If the version is
    None then the fields for the latest version are returned.

    The response format is important as it must match the requirements of reclineJS's field
    definitions. See http://okfnlabs.org/recline/docs/models.html#field for more details.

    All fields are returned by default as string types. This is because we have the capability to
    allow searchers to specify whether to treat a field as a string or a number when searching and
    therefore we don't need to try and guess the type and we can leave it to the user to know the
    type which won't cause problems like interpreting a field as a number when it shouldn't be (for
    example a barcode like '013655395'). If we decide that we do want to work out the type we simply
    need to add another step to this function where we count how many records in the version have
    the '.number' subfield - if the number is the same as the normal field count then the field is a
    number type, if not it's a string.

    The fields are returned in either alphabetical order, or if we have the ingestion details for
    the resource at the required version then the order of the fields will match the order of the
    fields in the original source.

    :param resource_id: the resource's id
    :param version: the version of the data we're querying (default: None, which means latest)
    :return: a list of dicts containing the field data
    '''
    # figure out the index name from the resource id
    index = prefix_resource(resource_id)
    # figure out the rounded version so that we can figure out the fields at the right version
    rounded_version = SEARCH_HELPER.get_rounded_versions([index], version)[index]
    # the key for caching should be unique to the resource and the rounded version
    cache_key = (resource_id, rounded_version)

    # if there is a cached version, return it! Woo!
    if cache_key in field_cache:
        return field_cache[cache_key]

    # create a list of field details, starting with the always present _id field
    fields = [{u'id': u'_id', u'type': u'integer'}]
    # lookup the mapping on elasticsearch to get all the field names
    mapping = CLIENT.indices.get_mapping(index)[index]
    # if the rounded version response is None that means there are no versions available which
    # shouldn't happen, but in case it does for some reason, just return the fields we have
    # already
    if rounded_version is None:
        return mapping, fields

    # retrieve all the resource's details up to the target version to get the column orders at each
    # version as they were in the ingestion sources for each version
    all_details = get_all_details(resource_id, up_to_version=version)
    # this set is used to avoid duplicating fields, we preload it with the _id column because we
    # want to ignore that (it's already in the fields list defined above)
    seen_fields = {u'_id'}
    field_names = []

    if all_details:
        # the all_details variable is an OrderedDict in ascending version order. We want to iterate
        # in descending version order though so that we respect the column order at the version
        # we're at before respecting any data from previous versions
        for details in reversed(all_details.values()):
            columns = [column for column in details.get_columns() if column not in seen_fields]
            field_names.extend(columns)
            seen_fields.update(columns)

    mapped_fields = mapping[u'mappings'][DOC_TYPE][u'properties'][u'data'][u'properties']
    # add any unseen mapped fields to the list of names. If we have a details object for each
    # version this shouldn't add any additional fields and if not it ensures we don't miss any
    field_names.extend(field for field in sorted(mapped_fields) if field not in seen_fields)

    if field_names:
        # find out which fields exist in this version and how many values each has
        search = MultiSearch(using=CLIENT, index=index)
        for field in field_names:
            # create a search which finds the documents that have a value for the given field at the
            # rounded version. We're only interested in the counts though so set size to 0
            search = search.add(Search().extra(size=0)
                                .filter(u'exists', **{u'field': prefix_field(field)})
                                .filter(u'term', **{u'meta.versions': rounded_version}))

        # run the search and get the response
        responses = search.execute()
        for i, response in enumerate(responses):
            # if the field has documents then it should be included in the fields list
            if response.hits.total > 0:
                fields.append({
                    u'id': field_names[i],
                    # by default everything is a string
                    u'type': u'string',
                })

    # stick the result in the cache for next time
    field_cache[cache_key] = (mapping, fields)

    return mapping, fields


def is_datastore_resource(resource_id):
    '''
    Looks up in elasticsearch whether there is an index for this resource or not and returns the
    boolean result. If there is an index, this is a datastore resource, if not it isn't.

    :param resource_id: the resource id
    :return: True if the resource is a datastore resource, False if not
    '''
    index_name = prefix_resource(resource_id)
    # check that the index for this resource exists and there is a reference to it in the status
    # index
    return CLIENT.indices.exists(index_name) and \
        index_name in SEARCH_HELPER.get_latest_index_versions([index_name])


def is_datastore_only_resource(resource_url):
    '''
    Checks whether the resource url is a datastore only resource url. When uploading data directly
    to the API without using a source file/URL the url of the resource will be set to
    "_datastore_only_resource" to indicate that as such. This function checks to see if the resource
    URL provided is one of these URLs. Note that we check a few different scenarios as CKAN has the
    nasty habit of adding a protocol onto the front of these URLs when saving the resource,
    sometimes.

    :param resource_url: the URL of the resource
    :return: True if the resource is a datastore only resource, False if not
    '''
    return (resource_url == DATASTORE_ONLY_RESOURCE or
            resource_url == u'http://{}'.format(DATASTORE_ONLY_RESOURCE) or
            resource_url == u'https://{}'.format(DATASTORE_ONLY_RESOURCE))


def is_ingestible(resource):
    """
    Returns True if the resource can be ingested into the datastore and False if not. To be
    ingestible the resource must either be a datastore only resource (signified by the url being
    set to _datastore_only_resource) or have a format that we can ingest (the format field on the
    resource is used for this, not the URL). If the url is None, False is returned. This is technically
    not possible due to a Resource model constraint but it's worth covering off anyway.

    :param resource: the resource dict
    :return: True if it is, False if not
    """
    if resource.get(u'url', None) is None:
        return False

    resource_format = resource.get(u'format', None)
    return (is_datastore_only_resource(resource[u'url']) or
            (resource_format is not None and resource_format.lower() in ALL_FORMATS) or
            (resource_format is not None and resource_format.lower() == u'zip'))


def get_public_alias_prefix():
    '''
    Returns the prefix to use for the public aliases.

    :return: the public prefix
    '''
    return u'pub'


def get_public_alias_name(resource_id):
    '''
    Returns the name of the alias which makes gives public access to this resource's datastore data.
    This is just "pub" (retrieved from get_public_alias_prefix above) prepended to the normal
    prefixed index name, for example:

        pubnhm-05ff2255-c38a-40c9-b657-4ccb55ab2feb

    :param resource_id: the resource's id
    :return: the name of the alias
    '''
    return u'{}{}'.format(get_public_alias_prefix(), prefix_resource(resource_id))


def trim_index_name(index_name):
    '''
    Given an index's name, remove the prefix returning the original resource id.

    :param index_name: the name of the index
    :return: the resource id
    '''
    return index_name[len(CONFIG.elasticsearch_index_prefix):]


def update_resources_privacy(package):
    '''
    Update the privacy of the resources in the datastore associated with the given package. If the
    privacy is already set correctly on each of the resource's indices in Elasticsearch this does
    nothing.

    :param package: the package model object (not the dict!)
    '''
    for resource in package.resources:
        update_privacy(resource.id, package.private)


def update_privacy(resource_id, is_private=None):
    '''
    Update the privacy of the given resource id in the datastore. If the privacy is already set
    correctly on the resource's index in Elasticsearch this does nothing.

    :param resource_id: the resource's id
    :param is_private: whether the package the resource is in is private or not. This is an optional
                       parameter, if it is left out we look up the resource's package in the
                       database and find out the private setting that way.
    :return: True if modifications were required to update the resource data's privacy, False if not
    '''
    if is_private is None:
        resource = model.Resource.get(resource_id)
        is_private = resource.package.private
    if is_private:
        return make_private(resource_id)
    else:
        return make_public(resource_id)


def make_private(resource_id):
    '''
    Makes the given resource private in elasticsearch. This is accomplished by removing the public
    alias for the resource. If the resource's base index doesn't exist at all, or the alias already
    doesn't exist, nothing happens.

    :param resource_id: the resource's id
    :return: True if modifications were required to make the resource's data private, False if not
    '''
    index_name = prefix_resource(resource_id)
    public_index_name = get_public_alias_name(resource_id)
    if CLIENT.indices.exists(index_name):
        if CLIENT.indices.exists_alias(index_name, public_index_name):
            CLIENT.indices.delete_alias(index_name, public_index_name)
            return True
    return False


def make_public(resource_id):
    '''
    Makes the given resource public in elasticsearch. This is accomplished by adding an alias to the
    resource's index. If the resource's base index doesn't exist at all or the alias already exists,
    nothing happens.

    :param resource_id: the resource's id
    :return: True if modifications were required to make the resource's data public, False if not
    '''
    index_name = prefix_resource(resource_id)
    public_index_name = get_public_alias_name(resource_id)
    if CLIENT.indices.exists(index_name):
        if not CLIENT.indices.exists_alias(index_name, public_index_name):
            actions = {
                u'actions': [
                    {u'add': {u'index': index_name, u'alias': public_index_name}}
                ]
            }
            CLIENT.indices.update_aliases(actions)
            return True
    return False


def is_resource_read_only(resource_id):
    '''
    Loops through the plugin implementations checking if any of them want the given resource id to
    be read only.

    :return: True if the resource should be treated as read only, False if not
    '''
    implementations = PluginImplementations(IVersionedDatastore)
    return any(plugin.datastore_is_read_only_resource(resource_id) for plugin in implementations)


class ReadOnlyResourceException(toolkit.ValidationError):
    pass


class InvalidVersionException(toolkit.ValidationError):
    pass


def iter_data_fields(mapping):
    '''
    Returns an iterator over the fields defined in the given mapping which yields the name of the
    field and the field's config. The names of the fields are represented by tuples allowing nested
    fields to be represented using their whole path (for example, a field at the top level is just
    ('field', ): {} but a nested one would be ('field', 'sub'): {}).

    :param mapping: the mapping dict returned from elasticsearch, this should be the first value in
                    the dict after the index name, i.e. the result of get_mapping(index)[index]
    :return: an iterator which yields fields and their configs
    '''
    def iter_properties(props, path=None):
        # this is a recursive function which can deal with nested fields
        if path is None:
            path = tuple()
        for field, config in props.items():
            if u'properties' in config:
                for result in iter_properties(config[u'properties'], path=path + (field, )):
                    yield result
            else:
                yield path + (field, ), config

    return iter_properties(mapping[u'mappings'][DOC_TYPE][u'properties'][u'data'][u'properties'])


def get_available_datastore_resources(context, only=None):
    '''
    Returns a set of resource ids accessible to the current user based on the given context that
    are also datastore resources. If the only parameter is passed then it is used to filter the set
    of resources that are returned to include only ones in the only list. If the parameter is not
    passed, or indeed is falsey in any way (such as an empty list) then all resource ids available
    to the user are returned.

    :param context: the dict ckan context to request auth against
    :param only: optional list of resource ids to filter the returned list by. Defaults to None
                 which indicates all available resources should be returned
    :return: a set of resource ids
    '''
    # retrieve all resource ids and associated package ids direct from the database for speed
    query = model.Session.query(model.Resource).join(model.Package) \
        .filter(model.Resource.state == u'active') \
        .filter(model.Package.state == u'active') \
        .with_entities(model.Resource.id, model.Package.id)
    # retrieve the names in the status index
    status_search = Search(index=CONFIG.elasticsearch_status_index_name, using=CLIENT) \
        .source([u'name'])

    if only:
        # apply filters to only get the resources passed in the only list
        query = query.filter(model.Resource.id.in_(only))
        status_search = status_search.filter(u'terms', name=only)

    # complete the database query and the elasticsearch query
    resources_and_packages = list(query)
    datastore_resources = {hit.name for hit in status_search.scan()}

    # this is the set of resource ids we will populate and return
    resource_ids = set()

    # when a resource is checked in ckan to see if can be accessed by the current user, it's package
    # is inspected. To avoid inspecting the same package over and over again when multiple resources
    # exist under a package, we'll cache the results
    package_access_cache = {}

    for resource_id, package_id in resources_and_packages:
        # if the resource isn't a datastore resource or we've already handled it, ignore it
        if resource_id not in datastore_resources or resource_id in resource_ids:
            continue

        # check the cache, we'll get back either True, False or None
        has_access = package_access_cache.get(package_id, None)

        if has_access:
            # access allowed, add to the list
            resource_ids.add(resource_id)
        elif has_access is None:
            # if the result of looking in the cache is None then there is no value for this package
            # in the cache and we need to do the work
            try:
                toolkit.check_access(u'package_show', context, {u'id': package_id})
                package_access_cache[package_id] = True
                # access allowed, add to the list
                resource_ids.add(resource_id)
            except toolkit.NotAuthorized:
                package_access_cache[package_id] = False
        else:
            # skip, there is no access
            continue

    return resource_ids


def get_last_after(result):
    '''
    Retrieves the "after" value from the last record in the list of hits.

    :param result: the result object from elasticsearch
    :return: a list or None
    '''
    if result.hits and u'sort' in result.hits[-1].meta:
        return list(result.hits[-1].meta[u'sort'])
    else:
        return None


def run_search(search, indexes, version=None):
    '''
    Convenience function to runs a search on the given indexes using the client available in this
    module.

    If the index(es) required for the search are missing then a CKAN SearchIndexError exception is
    raised.

    :param search: the elasticsearch-dsl search object
    :param indexes: either a list of index names to search in or a single index name as a string
    :param version: version to filter the search results to, optional
    :return: the result of running the query
    '''
    try:
        if version is not None:
            search = search.filter(create_version_query(version))
        if isinstance(indexes, basestring):
            indexes = [indexes]
        return search.index(indexes).using(CLIENT).execute()
    except NotFoundError as e:
        raise SearchIndexError(e.error)
