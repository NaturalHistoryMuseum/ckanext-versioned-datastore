from collections import defaultdict
from typing import Optional, List

from ckantools.decorators import action
from elasticsearch_dsl import Q, A
from splitgill.search import number, keyword

from ckanext.versioned_datastore.lib.query.schema import (
    validate_query,
    get_latest_query_version,
    hash_query,
)
from ckanext.versioned_datastore.lib.utils import (
    get_database,
    ivds_implementations,
    unprefix_index_name,
)
from ckanext.versioned_datastore.logic.multi import helptext, schema
from ckanext.versioned_datastore.logic.multi.groups import FieldGroups
from ckanext.versioned_datastore.logic.multi.utils import (
    make_request,
    get_available_datastore_resources,
)


@action(schema.vds_multi_query(), helptext.vds_multi_query, get=True)
def vds_multi_query(data_dict: dict):
    request = make_request(data_dict)
    response = request.run()
    result = {
        "total": response.count,
        "after": response.next_after,
        "records": [
            {
                "data": hit.data,
                "resource": hit.resource_id,
            }
            for hit in response.hits
        ],
    }

    for plugin in ivds_implementations():
        plugin.vds_after_multi_query(response, result)

    return result


@action(schema.vds_multi_count(), helptext.vds_multi_count, get=True)
def vds_multi_count(data_dict: dict):
    request = make_request(data_dict)
    request.set_no_results()
    # use an aggregation to get the hit count of each resource, set the size to the
    # number of resources we're querying to ensure we get all counts in one go and don't
    # have to paginate with a composite agg
    request.add_agg(
        "counts", "terms", field="_index", size=len(request.query.resource_ids)
    )
    response = request.run()

    # default the counts to 0 for all resources
    counts = {resource_id: 0 for resource_id in request.query.resource_ids}
    # then add the counts from the resources that matched the query
    counts.update(
        {
            unprefix_index_name(bucket["key"]): bucket["doc_count"]
            for bucket in response.aggs["counts"]["buckets"]
        }
    )
    return {"total": response.count, "counts": counts}


@action(
    schema.vds_multi_autocomplete_value(),
    helptext.vds_multi_autocomplete_value,
    get=True,
)
def vds_multi_autocomplete_value(
    data_dict: dict,
    field: str,
    prefix: Optional[str] = None,
    case_sensitive: bool = False,
):
    # extract the limit but default to a size of 20 if it's not present
    size = data_dict.pop("size", 20)
    # grab the after as we need to use it for the agg, not the query
    after = data_dict.pop("after", None)

    request = make_request(data_dict)
    request.set_no_results()

    # create the full path to the parsed field type we're going to filter and agg over
    field_path = keyword(field, case_sensitive=case_sensitive)

    if prefix:
        request.extra_filter &= Q("prefix", **{field_path: prefix})

    # add the aggregation which gets the field values
    request.add_agg(
        "field_values",
        "composite",
        # get one more than the requested size so that we can work out the after
        size=size + 1,
        sources={field: A("terms", field=field_path, order="asc")},
        # only include the after key if there is one
        **({"after": {field: after}} if after is not None else {}),
    )

    response = request.run()

    agg_result = response.aggs["field_values"]
    values = [bucket["key"][field] for bucket in agg_result["buckets"]]
    result = {"values": values[:size]}
    if "after_key" in agg_result and len(values) > size:
        result["after"] = agg_result["after_key"][field]
    return result


@action(
    schema.vds_multi_autocomplete_field(),
    helptext.vds_multi_autocomplete_field,
    get=True,
)
def vds_multi_autocomplete_field(
    resource_ids: List[str],
    text: str = "",
    lowercase: bool = False,
    version: Optional[int] = None,
):
    fields = defaultdict(dict)

    # if no resource IDs were provided, use all resources available to the user
    if not resource_ids:
        resource_ids = sorted(get_available_datastore_resources())

    for resource_id in resource_ids:
        database = get_database(resource_id)

        for field in database.get_parsed_fields(version=version):
            if text in (field.path.lower() if lowercase else field.path):
                fields[field.path][resource_id] = {
                    "name": field.name,
                    "path": field.path,
                    "count": field.count,
                    "text": field.count_text,
                    "boolean": field.count_boolean,
                    "date": field.count_date,
                    "number": field.count_number,
                    "geo": field.count_geo,
                }

    return {"count": len(fields), "fields": fields}


@action(schema.vds_multi_hash(), helptext.vds_multi_hash, get=True)
def vds_multi_hash(query: dict, query_version: Optional[str] = None):
    if query_version is None:
        query_version = get_latest_query_version()
    validate_query(query, query_version)
    return hash_query(query, query_version)


@action(schema.vds_multi_fields(), helptext.vds_multi_fields, get=True)
def vds_multi_fields(
    data_dict: dict, size: int = 10, ignore_groups: Optional[List[str]] = None
):
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
        fields = database.get_parsed_fields(version=request.query.version, query=query)
        field_groups.add(resource_id, fields)

    return field_groups.select(size)


@action(schema.vds_multi_stats(), helptext.vds_multi_stats, get=True)
def vds_multi_stats(data_dict: dict, field: str, missing: Optional[float] = None):
    """
    Retrieves a simple set of numerical stats about the given field and returns them in
    a dict. The stats provided are the same as the Elasticsearch stats aggregation
    (because that is what is uses!), so you'll get the following keys in the dict
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
    :return: a dict of statistical data
    """
    request = make_request(data_dict)
    request.set_no_results()
    agg_options = {"field": number(field)}
    if missing is not None:
        agg_options["missing"] = missing
    request.add_agg("field_stats", "stats", **agg_options)
    response = request.run()
    return response.aggs["field_stats"]
