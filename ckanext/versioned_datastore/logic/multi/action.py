from collections import defaultdict
from typing import List, Optional

from ckantools.decorators import action
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import A, MultiSearch, Q, Search
from splitgill.search import keyword, number, version_query

from ckanext.versioned_datastore.lib.query.schema import (
    get_latest_query_version,
    hash_query,
    validate_query,
)
from ckanext.versioned_datastore.lib.utils import (
    es_client,
    get_database,
    ivds_implementations,
    unprefix_index_name,
)
from ckanext.versioned_datastore.logic.multi import helptext, schema
from ckanext.versioned_datastore.logic.multi.groups import FieldGroups
from ckanext.versioned_datastore.logic.multi.utils import (
    get_available_datastore_resources,
    make_request,
)


@action(schema.vds_multi_query(), helptext.vds_multi_query, get=True)
def vds_multi_query(data_dict: dict):
    """
    Queries the given resources using the given query and at an optional version, and
    then return the results. Compared to the basic query, this action provides a richer
    query language and the ability to query multiple resources at the same time.

    :param data_dict: the data dict of options
    :returns: a dict which contains the total number of records found, an after value
        for pagination, and a list of dicts of record data
    """
    request = make_request(data_dict)
    response = request.run()
    result = {
        'total': response.count,
        'after': response.next_after,
        'records': [
            {
                'data': hit.data,
                'resource': hit.resource_id,
            }
            for hit in response.hits
        ],
    }

    for plugin in ivds_implementations():
        plugin.vds_after_multi_query(response, result)

    return result


@action(schema.vds_multi_count(), helptext.vds_multi_count, get=True)
def vds_multi_count(data_dict: dict):
    """
    Queries the given resources using the given query and at an optional version, and
    then returns the total number of records which matched the query and the counts per
    resource. Compared to the basic query count, this action provides a richer query
    language and the ability to query multiple resources at the same time.

    :param data_dict: the data dict of options
    :returns: a dict which contains the total count and a breakdown of the hits per
        resource
    """
    request = make_request(data_dict)
    request.set_no_results()
    # use an aggregation to get the hit count of each resource, set the size to the
    # number of resources we're querying to ensure we get all counts in one go and don't
    # have to paginate with a composite agg
    request.add_agg(
        'counts', 'terms', field='_index', size=len(request.query.resource_ids)
    )
    response = request.run()

    # default the counts to 0 for all resources
    counts = {resource_id: 0 for resource_id in request.query.resource_ids}
    # then update with the counts from the resources that matched the query
    for bucket in response.aggs['counts']['buckets']:
        counts[unprefix_index_name(bucket['key'])] += bucket['doc_count']
    return {'total': response.count, 'counts': counts}


@action(
    schema.vds_multi_autocomplete_value(),
    helptext.vds_multi_autocomplete_value,
    get=True,
)
def vds_multi_autocomplete_value(
    data_dict: dict,
    field: str,
    prefix: Optional[str] = None,
):
    """
    Queries the given resources using the given query and at an optional version, and
    then returns the values that match the given prefix for the given field. The list of
    values is returned.

    :param data_dict: the action options
    :param field: the field to get the values from
    :param prefix: the prefix to search for
    :returns: a dict containing the list of values and an after key if there are more
        values available. If the after key is not present, there are no more values
        available
    """
    # extract the limit but default to a size of 20 if it's not present
    size = data_dict.pop('size', 20)
    # grab the after as we need to use it for the agg, not the query
    after = data_dict.pop('after', None)

    request = make_request(data_dict)
    request.set_no_results()

    # create the full path to the parsed field type we're going to filter and agg over
    field_path = keyword(field)

    if prefix:
        request.extra_filter &= Q('prefix', **{field_path: prefix})

    # add the aggregation which gets the field values
    request.add_agg(
        'field_values',
        'composite',
        # get one more than the requested size so that we can work out the after
        size=size + 1,
        sources={field: A('terms', field=field_path, order='asc')},
        # only include the after key if there is one
        **({'after': {field: after}} if after is not None else {}),
    )

    response = request.run()

    agg_result = response.aggs['field_values']
    values = [bucket['key'][field] for bucket in agg_result['buckets']]
    result = {'values': values[:size]}
    if 'after_key' in agg_result and len(values) > size:
        result['after'] = agg_result['after_key'][field]
    return result


@action(
    schema.vds_multi_autocomplete_field(),
    helptext.vds_multi_autocomplete_field,
    get=True,
)
def vds_multi_autocomplete_field(
    resource_ids: List[str],
    text: str = '',
    lowercase: bool = False,
    version: Optional[int] = None,
):
    """
    Queries the field names in the given resources, filtering by checking if the field
    name contains the text parameter.

    The fields key in the response dict contains the field's full path, and then for
    each resource ID the field path appears in:
        - the field's name
        - the field's path
        - the number of records which have this field
        - the number of records which have a text parsed value in this field
        - the number of records which have a keyword parsed value in this field
        - the number of records which have a boolean parsed value in this field
        - the number of records which have a date parsed value in this field
        - the number of records which have a number parsed value in this field
        - the number of records which have a geo parsed value in this field

    Note that for large numbers of resources, this action is quite expensive to run.

    :param resource_ids: the resources match fields on (if no resource IDs are passed,
                         all resources are searched)
    :param text: the text to search for
    :param lowercase: whether to compare the text to the field names in lowercase
                      (default is False)
    :param version: the version to search at (default is None, which means latest)
    :returns: the total number of fields matched and details about the fields that were
             matched
    """
    fields = defaultdict(dict)

    # if no resource IDs were provided, use all resources available to the user
    if not resource_ids:
        resource_ids = sorted(get_available_datastore_resources())

    for resource_id in resource_ids:
        database = get_database(resource_id)

        try:
            parsed_fields = database.get_parsed_fields(version=version)
        except NotFoundError:
            # temporary fix for splitgill#38 (so we can ignore unavailable resources)
            continue

        for field in parsed_fields:
            if text in (field.path.lower() if lowercase else field.path):
                fields[field.path][resource_id] = {
                    'name': field.name,
                    'path': field.path,
                    'count': field.count,
                    'text': field.count_text,
                    'keyword': field.count_keyword,
                    'boolean': field.count_boolean,
                    'date': field.count_date,
                    'number': field.count_number,
                    'geo': field.count_geo,
                }

    return {'count': len(fields), 'fields': fields}


@action(schema.vds_multi_hash(), helptext.vds_multi_hash, get=True)
def vds_multi_hash(query: dict, query_version: Optional[str] = None):
    """
    Given a query and a query schema version, hash them and return it.

    :param query: the query
    :param query_version: the version of the query schema
    :returns: the hash
    """
    if query_version is None:
        query_version = get_latest_query_version()
    validate_query(query, query_version)
    return hash_query(query, query_version)


@action(schema.vds_multi_fields(), helptext.vds_multi_fields, get=True)
def vds_multi_fields(
    data_dict: dict, size: int = 10, ignore_groups: Optional[List[str]] = None
):
    """
    Groups the fields available on the given resources at the optional given version and
    returns as many of them as requested in the size parameter. The groups are created
    by matching field names across resources and then sorted in descending order with
    the most common groups at the top (both most common in terms of resources containing
    the field, but also most common as in with the most records that have the field in
    them in a resource).

    :param data_dict: the action options
    :param size: the number of field groups to return (defaults to the top 10)
    :param ignore_groups: an optional list of fields to ignore
    :returns: a list of field groups represented as dicts
    """
    request = make_request(data_dict)
    request.set_no_results()

    query = request.query.to_dsl()

    field_groups = FieldGroups(skip_ids=True)
    if ignore_groups:
        for ignore in ignore_groups:
            field_groups.ignore(ignore)

    for plugin in ivds_implementations():
        plugin.vds_modify_field_groups(request.query.resource_ids, field_groups)

    for resource_id in request.query.resource_ids:
        database = get_database(resource_id)
        try:
            fields = database.get_parsed_fields(
                version=request.query.version, query=query
            )
        except NotFoundError:
            # temporary fix for splitgill#38 (so we can ignore unavailable resources)
            continue
        field_groups.add(resource_id, fields)

    return field_groups.select(size)


@action(schema.vds_multi_stats(), helptext.vds_multi_stats, get=True)
def vds_multi_stats(data_dict: dict, field: str, missing: Optional[float] = None):
    """
    Retrieves a simple set of numerical stats about the given field and returns them in
    a dict. The stats provided are the same as the Elasticsearch stats aggregation
    (because that is what is used!), so you'll get the following keys in the dict
    response:

      - count
      - min
      - max
      - avg
      - sum

    The missing parameter defines how documents that are missing a value should be
    treated. By default, they will be ignored, but it is also possible to treat them as
    if they had a value by providing one here.

    :param data_dict: the data dict passed to this action
    :param field: the field to get stats for
    :param missing: value to use for records missing this field, or None to ignore them
    :returns: a dict of statistical data
    """
    request = make_request(data_dict)
    request.set_no_results()
    agg_options = {'field': number(field)}
    if missing is not None:
        agg_options['missing'] = missing
    request.add_agg('field_stats', 'stats', **agg_options)
    response = request.run()
    return response.aggs['field_stats']


@action(schema.vds_multi_direct(), helptext.vds_multi_direct)
def vds_multi_direct(data_dict: dict):
    """
    Allows users to run Elasticsearch queries directly against the cluster. The raw
    response from Elasticsearch is returned directly as well. This is locked down to
    admin users only to avoid misuse, but can be used from other extensions easily with
    the ignore_auth context option.

    To control the version that is searched, either include it in the search object
    directly, or:
        - pass "latest", None, or don't include the version key in the data dict to
          search the latest version
        - pass "all" to search all versions
        - pass a version timestamp value to search at a specific version

    A list of resource IDs must be passed and if no resource IDs are passed an error is
    raised during validation.

    :param data_dict: the action data dict
    :returns: the raw response from Elasticsearch
    """
    resource_ids = data_dict['resource_ids']
    version = data_dict.get('version', 'latest')
    search = Search.from_dict(data_dict.get('search', {}))

    databases = map(get_database, resource_ids)
    if version == 'latest':
        indices = [database.indices.latest for database in databases]
    else:
        indices = [database.indices.wildcard for database in databases]
        if version is not None and version != 'all':
            search = search.filter(version_query(int(version)))

    # call search.index() empty first to reset the indices thus avoiding them being set
    # in the passed search dict
    search = search.index().index(indices)

    multi_search = MultiSearch(using=es_client()).add(search)
    result = next(iter(multi_search.execute()))

    return result.to_dict()
