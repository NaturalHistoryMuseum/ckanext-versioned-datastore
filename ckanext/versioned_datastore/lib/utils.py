from eevee.config import Config
from eevee.indexing.utils import DOC_TYPE
from eevee.search.search import Searcher

# TODO: jazz up the config
config = Config(elasticsearch_hosts=[u'http://172.17.0.3:9200'])
searcher = Searcher(config)


def format_facets(aggs):
    '''
    Formats the facet aggregation result into the format we require. Specifically we expand the
    buckets out into a dict that looks like this:

        {
            "facet1": {
                "value1": 1,
                "value2": 4,
                "value3": 1,
                "value4": 2,
            },
            "facet2": {
                "value1": 9,
                "value2": 10,
            }
        }

    etc.

    :param aggs: the aggregation dict returned from eevee/elasticsearch
    :return: the facet information as a dict
    '''
    facets = {}
    for facet, details in aggs.items():
        facets[facet] = {value_details['key']: value_details['doc_count']
                         for value_details in details['buckets']}
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
    # TODO: move to eevee?
    # the index name for the resource is prefixed
    index = '{}{}'.format(config.elasticsearch_index_prefix, resource_id)
    # lookup the mapping on elasticsearch
    mapping = searcher.elasticsearch.indices.get_mapping(index)
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
    index = '{}{}'.format(config.elasticsearch_index_prefix, resource_id)
    return searcher.elasticsearch.indices.exists(index)
