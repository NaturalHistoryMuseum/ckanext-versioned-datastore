import tempfile
from contextlib import contextmanager, closing

import requests
import rq
from eevee.indexing.utils import DOC_TYPE
from eevee.search.search import Searcher

from ckan import plugins
from ckan.lib.navl import dictization_functions
from ckanext.rq import jobs


CSV_FORMATS = [u'csv', u'application/csv']
TSV_FORMATS = [u'tsv']
XLS_FORMATS = [u'xls', u'application/vnd.ms-excel']
XLSX_FORMATS = [u'xlsx', u'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
ALL_FORMATS = CSV_FORMATS + TSV_FORMATS + XLS_FORMATS + XLSX_FORMATS

CONFIG = None
SEARCHER = None


def setup_searcher(config):
    global CONFIG
    global SEARCHER
    CONFIG = config
    SEARCHER = Searcher(config)


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
            'details': {
                'sum_other_doc_count': details['sum_other_doc_count'],
                'doc_count_error_upper_bound': details['doc_count_error_upper_bound'],
            },
            'values': {value_details['key']: value_details['doc_count']
                       for value_details in details['buckets']}
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
        for field in mappings['mappings'][DOC_TYPE]['properties']['data']['properties']:
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
    return (resource[u'url'] == u'_datastore_only_resource' or
            resource[u'url'] == u'http://_datastore_only_resource' or
            (resource_format is not None and resource_format.lower() in ALL_FORMATS))


@contextmanager
def download_to_temp_file(url):
    """
    Streams the data from the given URL and saves it in a temporary file. The (named) temporary file
    is then yielded to the caller for use. Once the context collapses the temporary file is removed.

    :param url: the url to stream the data from
    """
    # open up the url for streaming
    with closing(requests.get(url, stream=True)) as r:
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


def ensure_importing_queue_exists():
    '''
    This is a massive hack to get around the lack of rq Queue kwarg exposure from ckanext-rq. The
    default timeout for queues is 180 seconds in rq which is not long enough for our import tasks
    but the timeout parameter hasn't been exposed. This code creates a new queue in the ckanext-rq
    cache so that when enqueuing new jobs it is used rather than a default one. Once this bug has
    been fixed in ckan/ckanext-rq this code will be removed.
    '''
    name = jobs.add_queue_name_prefix(u'importing')
    # set the timeout to 12 hours
    queue = rq.Queue(name, default_timeout=60 * 60 * 12, connection=jobs._connect())
    jobs._queues[name] = queue
