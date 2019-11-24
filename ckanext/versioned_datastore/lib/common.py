import logging

from eevee.config import Config
from eevee.search import SearchHelper

log = logging.getLogger(__name__)
# stop elasticsearch from showing warning logs
logging.getLogger(u'elasticsearch').setLevel(logging.ERROR)


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
ES_CLIENT = None


def setup(ckan_config):
    '''
    Given the CKAN config, setup the plugin's global variables.

    :param ckan_config: the ckan config
    '''
    global CONFIG, SEARCH_HELPER, ES_CLIENT

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
    ES_CLIENT = SEARCH_HELPER.client
