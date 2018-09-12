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


def _extract_details(field, details):
    '''
    Helper that produces a dict with the field and it's type details.

    :param field: the field name
    :param details: the details dict for that field
    :return: a dict
    '''
    return {
        'name': field,
        'type': details['type'],
    }


# TODO: should probs cache this
def get_fields(resource_id):
    '''
    Given a resource id, looks up the mapping in elasticsearch for the index that contains the
    resource's data using the searcher object's client and then returns a list of fields with type
    information.

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
        for field, details in mappings['mappings'][DOC_TYPE]['properties']['data']['properties'].items():
            # if the field is a nested object then it'll have its own properties, only go one level
            # deep though for now
            if 'properties' in details:
                for subfield, subdetails in details['properties'].items():
                    # add the details and make sure to prefix the field with the parent field name
                    fields.append(_extract_details('{}.{}'.format(field, subfield), subdetails))
            else:
                fields.append(_extract_details(field, details))
    return fields


def is_datastore_resource(resource_id):
    '''
    Looks up in elasticsearch whether there is an index for this resource or not and returns the
    boolean result. If there is an index, this is a datastore resource, if not it isn't.

    :param resource_id: the resource id
    :return: True if the resource is a datastore resource, False if not
    '''
    index = '{}{}'.format(config.elasticsearch_index_prefix, resource_id)
    return searcher.elasticsearch.indices.exists(index)
