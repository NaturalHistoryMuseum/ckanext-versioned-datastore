import tempfile
from contextlib import contextmanager, closing

import requests
from eevee.config import Config
from eevee.indexing.utils import DOC_TYPE
from eevee.search.search import Searcher

from ckan import plugins
from ckan.lib.navl import dictization_functions

DATASTORE_ONLY_RESOURCE = u'_datastore_only_resource'
CSV_FORMATS = [u'csv', u'application/csv']
TSV_FORMATS = [u'tsv']
XLS_FORMATS = [u'xls', u'application/vnd.ms-excel']
XLSX_FORMATS = [u'xlsx', u'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
ALL_FORMATS = CSV_FORMATS + TSV_FORMATS + XLS_FORMATS + XLSX_FORMATS

CONFIG = None
SEARCHER = None


def setup_eevee(ckan_config):
    '''
    Given the CKAN config, create the Eevee config object and the eevee Searcher object.

    :param ckan_config: the ckan config
    '''
    global CONFIG
    global SEARCHER

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
    SEARCHER = Searcher(CONFIG)


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
    data_dict, errors = dictization_functions.validate(data_dict, schema, context)
    if errors:
        raise plugins.toolkit.ValidationError(errors)
    return data_dict


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


# TODO: should probs cache this
def get_fields(resource_id):
    '''
    Given a resource id, looks up the mapping in elasticsearch for the index that contains the
    resource's data using the searcher object's client and then returns a list of fields with type
    information.

    The response format is important as it must match the requirements of reclineJS's field
    definitions. See http://okfnlabs.org/recline/docs/models.html#field for more details.

    :param resource_id:
    :return:
    '''
    # TODO: return only the fields in use in the version being searched
    # the index name for the resource is prefixed
    index = SEARCHER.prefix_index(resource_id)
    # lookup the mapping on elasticsearch
    mapping = SEARCHER.elasticsearch.indices.get_mapping(index)
    fields = []
    for mappings in mapping.values():
        # we're only going to return the details of the data fields, so loop over those properties
        for field in mappings[u'mappings'][DOC_TYPE][u'properties'][u'data'][u'properties']:
            fields.append({
                u'id': field,
                # by default, everything is a string
                u'type': u'string',
            })
    return mapping, fields


def is_datastore_resource(resource_id):
    '''
    Looks up in elasticsearch whether there is an index for this resource or not and returns the
    boolean result. If there is an index, this is a datastore resource, if not it isn't.

    :param resource_id: the resource id
    :return: True if the resource is a datastore resource, False if not
    '''
    return SEARCHER.elasticsearch.indices.exists(SEARCHER.prefix_index(resource_id))


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
    resource is used for this, not the URL).

    :param resource: the resource dict
    :return: True if it is, False if not
    """
    resource_format = resource.get(u'format', None)
    return (is_datastore_only_resource(resource[u'url']) or
            (resource_format is not None and resource_format.lower() in ALL_FORMATS))


@contextmanager
def download_to_temp_file(url, headers=None):
    """
    Streams the data from the given URL and saves it in a temporary file. The (named) temporary file
    is then yielded to the caller for use. Once the context collapses the temporary file is removed.

    :param url: the url to stream the data from
    :param headers: a dict of headers to pass with the request
    """
    headers = headers if headers else {}
    # open up the url for streaming
    with closing(requests.get(url, stream=True, headers=headers)) as r:
        # create a temporary file to store the data in
        with tempfile.NamedTemporaryFile() as temp:
            # iterate over the data from the url stream in chunks
            for chunk in r.iter_content(chunk_size=1024):
                # only write chunks with data in them
                if chunk:
                    # write the chunk to the file
                    temp.write(chunk)
            # the url has been completely downloaded to the temp file, so yield it for use
            temp.seek(0)
            yield temp
