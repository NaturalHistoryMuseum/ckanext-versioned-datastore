from eevee.config import Config
from eevee.indexing.utils import DOC_TYPE
from eevee.search.search import Searcher

from ckan import plugins
from ckan.lib.navl import dictization_functions


searcher = None


def get_searcher():
    # TODO: jazz up the config
    global searcher
    if searcher is None:
        searcher = Searcher(Config(elasticsearch_hosts=[u'http://172.17.0.3:9200']))
    return searcher


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
    # the index name for the resource is prefixed
    index = get_searcher().prefix_index(resource_id)
    # lookup the mapping on elasticsearch
    mapping = get_searcher().elasticsearch.indices.get_mapping(index)
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
    return get_searcher().elasticsearch.indices.exists(get_searcher().prefix_index(resource_id))
