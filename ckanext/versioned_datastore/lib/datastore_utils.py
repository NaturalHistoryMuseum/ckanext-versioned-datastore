from ckan import model
from ckan.plugins import toolkit, PluginImplementations
from splitgill.indexing.utils import DOC_TYPE
import time

from . import common
from ..interfaces import IVersionedDatastore


def get_latest_version(resource_id):
    """
    Retrieves the latest version of the given resource from the status index.

    :param resource_id: the resource's id
    :return: the version or None if the resource isn't indexed
    """
    index_name = prefix_resource(resource_id)
    return common.SEARCH_HELPER.get_latest_index_versions([index_name]).get(
        index_name, None
    )


def prefix_resource(resource_id):
    """
    Adds the configured prefix to the start of the resource id to get the index name for
    the resource data in elasticsearch.

    :param resource_id: the resource id
    :return: the resource's index name
    """
    return f'{common.CONFIG.elasticsearch_index_prefix}{resource_id}'


def unprefix_index(index_name):
    """
    Removes the configured prefix from the start of the index name to get the resource
    id.

    :param index_name: the index name
    :return: the resource's id
    """
    return index_name[len(common.CONFIG.elasticsearch_index_prefix) :]


def prefix_field(field):
    """
    Prefixes a the given field name with "data.". All data from the resource in
    splitgill is stored under the data key in the elasticsearch record so to avoid end
    users needing to know that all fields should be referenced by their non-
    data.-prefixed name until they are internal to the code and can be prefixed before
    being passed on to splitgill.

    :param field: the field name
    :return: data.<field>
    """
    return f'data.{field}'


def get_public_alias_prefix():
    """
    Returns the prefix to use for the public aliases.

    :return: the public prefix
    """
    return 'pub'


def get_public_alias_name(resource_id):
    """
    Returns the name of the alias which makes gives public access to this resource's
    datastore data. This is just "pub" (retrieved from get_public_alias_prefix above)
    prepended to the normal prefixed index name, for example:

        pubnhm-05ff2255-c38a-40c9-b657-4ccb55ab2feb

    :param resource_id: the resource's id
    :return: the name of the alias
    """
    return f'{get_public_alias_prefix()}{prefix_resource(resource_id)}'


def trim_index_name(index_name):
    """
    Given an index's name, remove the prefix returning the original resource id.

    :param index_name: the name of the index
    :return: the resource id
    """
    return index_name[len(common.CONFIG.elasticsearch_index_prefix) :]


def update_resources_privacy(package):
    """
    Update the privacy of the resources in the datastore associated with the given
    package. If the privacy is already set correctly on each of the resource's indices
    in Elasticsearch this does nothing.

    :param package: the package model object (not the dict!)
    """
    for resource in package.resources:
        update_privacy(resource.id, package.private)


def update_privacy(resource_id, is_private=None):
    """
    Update the privacy of the given resource id in the datastore. If the privacy is
    already set correctly on the resource's index in Elasticsearch this does nothing.

    :param resource_id: the resource's id
    :param is_private: whether the package the resource is in is private or not. This is an optional
                       parameter, if it is left out we look up the resource's package in the
                       database and find out the private setting that way.
    :return: True if modifications were required to update the resource data's privacy, False if not
    """
    if is_private is None:
        resource = model.Resource.get(resource_id)
        is_private = resource.package.private
    if is_private:
        return make_private(resource_id)
    else:
        return make_public(resource_id)


def make_private(resource_id):
    """
    Makes the given resource private in elasticsearch. This is accomplished by removing
    the public alias for the resource. If the resource's base index doesn't exist at
    all, or the alias already doesn't exist, nothing happens.

    :param resource_id: the resource's id
    :return: True if modifications were required to make the resource's data private, False if not
    """
    index_name = prefix_resource(resource_id)
    public_index_name = get_public_alias_name(resource_id)
    if common.ES_CLIENT.indices.exists(index_name):
        if common.ES_CLIENT.indices.exists_alias(index_name, public_index_name):
            common.ES_CLIENT.indices.delete_alias(index_name, public_index_name)
            return True
    return False


def make_public(resource_id):
    """
    Makes the given resource public in elasticsearch. This is accomplished by adding an
    alias to the resource's index. If the resource's base index doesn't exist at all or
    the alias already exists, nothing happens.

    :param resource_id: the resource's id
    :return: True if modifications were required to make the resource's data public, False if not
    """
    index_name = prefix_resource(resource_id)
    public_index_name = get_public_alias_name(resource_id)
    if common.ES_CLIENT.indices.exists(index_name):
        if not common.ES_CLIENT.indices.exists_alias(index_name, public_index_name):
            actions = {
                'actions': [{'add': {'index': index_name, 'alias': public_index_name}}]
            }
            common.ES_CLIENT.indices.update_aliases(actions)
            return True
    return False


def is_resource_read_only(resource_id):
    """
    Loops through the plugin implementations checking if any of them want the given
    resource id to be read only.

    :return: True if the resource should be treated as read only, False if not
    """
    implementations = PluginImplementations(IVersionedDatastore)
    return any(
        plugin.datastore_is_read_only_resource(resource_id)
        for plugin in implementations
    )


class ReadOnlyResourceException(toolkit.ValidationError):
    pass


class InvalidVersionException(toolkit.ValidationError):
    pass


def iter_data_fields(mapping):
    """
    Returns an iterator over the fields defined in the given mapping which yields the
    name of the field and the field's config. The names of the fields are represented by
    tuples allowing nested fields to be represented using their whole path (for example,
    a field at the top level is just ('field', ): {} but a nested one would be ('field',
    'sub'): {}).

    :param mapping: the mapping dict returned from elasticsearch, this should be the first value in
                    the dict after the index name, i.e. the result of get_mapping(index)[index]
    :return: an iterator which yields fields and their configs
    """

    def iter_properties(props, path=None):
        # this is a recursive function which can deal with nested fields
        if path is None:
            path = tuple()
        for field, config in props.items():
            if 'properties' in config:
                for result in iter_properties(
                    config['properties'], path=path + (field,)
                ):
                    yield result
            else:
                yield path + (field,), config

    return iter_properties(
        mapping['mappings'][DOC_TYPE]['properties']['data']['properties']
    )


def get_last_after(hits):
    """
    Retrieves the "sort" value from the last record in passed the list of hits.

    :param hits: a list of hits from an elasticsearch response
    :return: a list or None
    """
    if hits and 'sort' in hits[-1].meta:
        return list(hits[-1].meta['sort'])
    else:
        return None


def is_datastore_resource(resource_id):
    """
    Looks up in elasticsearch whether there is an index for this resource or not and
    returns the boolean result. If there is an index, this is a datastore resource, if
    not it isn't.

    :param resource_id: the resource id
    :return: True if the resource is a datastore resource, False if not
    """
    index_name = prefix_resource(resource_id)
    # check that the index for this resource exists and there is a reference to it in the status
    # index
    return common.ES_CLIENT.indices.exists(
        index_name
    ) and index_name in common.SEARCH_HELPER.get_latest_index_versions([index_name])


def is_datastore_only_resource(resource_url):
    """
    Checks whether the resource url is a datastore only resource url. When uploading
    data directly to the API without using a source file/URL the url of the resource
    will be set to "_datastore_only_resource" to indicate that as such. This function
    checks to see if the resource URL provided is one of these URLs. Note that we check
    a few different scenarios as CKAN has the nasty habit of adding a protocol onto the
    front of these URLs when saving the resource, sometimes.

    :param resource_url: the URL of the resource
    :return: True if the resource is a datastore only resource, False if not
    """
    return (
        resource_url == common.DATASTORE_ONLY_RESOURCE
        or resource_url == f'http://{common.DATASTORE_ONLY_RESOURCE}'
        or resource_url == f'https://{common.DATASTORE_ONLY_RESOURCE}'
    )


def is_ingestible(resource):
    """
    Returns True if the resource can be ingested into the datastore and False if not. To
    be ingestible the resource must either be a datastore only resource (signified by
    the url being set to _datastore_only_resource) or have a format that we can ingest
    (the format field on the resource is used for this, not the URL). If the url is
    None, False is returned. This is technically not possible due to a Resource model
    constraint but it's worth covering off anyway.

    :param resource: the resource dict
    :return: True if it is, False if not
    """
    if resource.get('url', None) is None:
        return False

    resource_format = resource.get('format', None)
    return (
        is_datastore_only_resource(resource['url'])
        or (
            resource_format is not None
            and resource_format.lower() in common.ALL_FORMATS
        )
        or (resource_format is not None and resource_format.lower() == 'zip')
    )


def get_queue_length(queue_name):
    """
    This is a *very* hacky way to get the length of a queue, including anything already
    processing.

    :param queue_name: the name of the queue to check, e.g. 'download'
    :return: length of queue as int
    """
    # because only pending jobs are counted, not active/running, if you add to the queue
    # and job_list can't see it, the queue was empty; if it can, something else is
    # already running.
    def _temp_job():
        time.sleep(1)

    job = toolkit.enqueue_job(
        _temp_job,
        queue=queue_name,
        title=f'{queue_name} queue status test',
        rq_kwargs={'ttl': '1s'},
    )

    queued_jobs = toolkit.get_action('job_list')(
        {'ignore_auth': True}, {'queues': [queue_name]}
    )

    job.delete()

    return len(queued_jobs)


def get_es_health():
    return {'ping': common.ES_CLIENT.ping(), 'info': common.ES_CLIENT.info()}
