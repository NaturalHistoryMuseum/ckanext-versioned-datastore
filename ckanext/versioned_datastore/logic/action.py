# -*- coding: utf-8 -*-
import copy
import logging
from collections import defaultdict

import jsonschema
from ckan.plugins import toolkit, PluginImplementations
from ckanext.versioned_datastore.interfaces import IVersionedDatastore
from ckanext.versioned_datastore.lib import utils, stats
from ckanext.versioned_datastore.lib.importing import check_version_is_valid
from ckanext.versioned_datastore.lib.indexing.indexing import DatastoreIndex
from ckanext.versioned_datastore.lib.query import get_latest_query_version, \
    InvalidQuerySchemaVersionError, create_search_and_slug, resolve_slug
from ckanext.versioned_datastore.lib.queuing import queue_index, queue_import, queue_deletion
from ckanext.versioned_datastore.lib.search import create_search, prefix_field
from ckanext.versioned_datastore.logic import schema
from datetime import datetime
from eevee.indexing.utils import DOC_TYPE
from eevee.utils import to_timestamp
from eevee.search import create_version_query
from elasticsearch import RequestError
from elasticsearch_dsl import A, Search, MultiSearch

log = logging.getLogger(__name__)
# stop elasticsearch from showing warning logs
logging.getLogger(u'elasticsearch').setLevel(logging.ERROR)


@toolkit.side_effect_free
def datastore_search(context, data_dict):
    '''
    This action allows you to search data in a resource. It is designed to function in a similar way
    to CKAN's core datastore_search but with a few extra bells and whistles, most prominently
    versioning. This allows the resource to be searched at any moment in it's lifespan and have the
    data as it looked at that moment returned, even if it has changed since.

    If the resource to be searched is private then appropriate authorization is required.

    Note that in the parameters listed below spaces should only be included if part of a field name,
    so, for example, don't include any spaces in comma separated lists unless needed.

    :param resource_id: id of the resource to be searched against
    :type resource_id: string
    :param q: full text query. If a string is passed, all fields are searched with the value. If a
          dict is passed each of the fields and values contained within will be searched as
          required (e.g. {"field1": "a", "field2": "b"}).
    :type q: string or dictionary
    :param filters: a dictionary of conditions that must be met to match a record
                    (e.g {"field1": "a", "field2": "b"}) (optional)
    :type filters: dictionary
    :param after: search_after value for elasticsearch to paginate from (optional). Use this
                  mechanism to do deep (beyond 10000 values) pagination. The values have to match
                  the sort currently in use and therefore it's recommended that this value is not
                  built but rather passed from the previous result's 'after' key.
    :type after: a list of values
    :param limit: maximum number of records to return (optional, default: 100)
    :type limit: int
    :param offset: offset this number of records (optional)
    :type offset: int
    :param fields: fields to return for each record (optional, default: all fields are returned)
    :type fields: list or comma separated string
    :param sort: list of field names with ordering. Ordering is ascending by default, if descending
                 is required, add "desc" after the field name
                 e.g.: "fieldname1,fieldname2 desc" sorts by fieldname1 asc and fieldname2 desc
    :type sort: list or comma separated string
    :param version: version to search at, if not provided the current version of the data is
                   searched.
    :type version: int, number of milliseconds (not seconds!) since UNIX epoch
    :param facets: if present, the top 10 most frequent values for each of the fields in this list
                   will be returned along with estimated counts for each value. Calculating these
                   results has a reasonable overhead so only include this parameter if you need it
    :type facets: list or comma separated string
    :param facet_limits: if present, specifies the number of top values to retrieve for the facets
                        listed within. The default number will be used if this parameter is not
                        specified or if a facet in the facets list does not appear in this dict. For
                        example, with this facet list ['facet1', 'facet2', 'facet3', 'facet4'], and
                        this facet_limits dict: {'facet1': 50, 'facet4': 10}, facet1 and facet4
                        would be limited to top 50 and 10 values respectively, whereas facet2 and
                        facet3 would be limited to the default of the top 10.
    :type facet_limits: a dict
    :param run_query: boolean value indicating whether the query should be run and the results
                      returned or whether the query should be created and the elasticsearch query
                      returned instead of the results. Defaults to True.
    :type run_query: boolean


    **Results:**

    The result of this action is a dictionary with the following keys:

    :rtype: A dict with the following keys
    :param fields: fields/columns and their extra metadata
    :type fields: list of dicts
    :param total: number of total matching records
    :type total: int
    :param records: list of matching results
    :type records: list of dicts
    :param facets: list of fields and their top 10 values, if requested
    :type facets: dict
    :param after: the next page's search_after value which can be passed back as the "after"
                  parameter. This value will always be included if there were results otherwise None
                  is returned. A value will also always be returned even if this page is the last.
    :type after: a list or None

    If run_query is True, then a dict with the following keys is returned instead:

    :param indexes: a list of the fully qualified indexes that the query would have been run against
    :type indexes: a list of strings
    :param search: the query dict that would have been sent to elasticsearch
    :type search: dict

    In addition to returning these result dicts, the actual result object is made available through
    the context dict under the key "versioned_datastore_query_result". This isn't available through
    the http action API however.
    '''
    original_data_dict, data_dict, version, search = create_search(context, data_dict)
    resource_id = data_dict[u'resource_id']
    index_name = utils.prefix_resource(resource_id)

    # if the version is None, default it to the current timestamp
    if version is None:
        version = to_timestamp(datetime.now())

    # add the version filter to the query
    search = search.filter(create_version_query(version))

    # if the run query option is false (default to true if not present) then just return the query
    # we would have run against elasticsearch instead of actually running it. This is useful for
    # running the query outside of ckan, for example on a tile server.
    if not data_dict.get(u'run_query', True):
        return {
            u'indexes': [index_name],
            u'search': search.to_dict(),
        }
    else:
        result = utils.run_search(search, [index_name])

        # allow other extensions implementing our interface to modify the result
        for plugin in PluginImplementations(IVersionedDatastore):
            result = plugin.datastore_modify_result(context, original_data_dict, data_dict, result)

        # add the actual result object to the context in case the caller is an extension and they
        # have used one of the interface hooks to alter the search object and include, for example,
        # an aggregation
        context[u'versioned_datastore_query_result'] = result

        # get the fields
        mapping, fields = utils.get_fields(resource_id, version)
        # allow other extensions implementing our interface to modify the field definitions
        for plugin in PluginImplementations(IVersionedDatastore):
            fields = plugin.datastore_modify_fields(resource_id, mapping, fields)

        # return a dictionary containing the results and other details
        return {
            u'total': result.hits.total,
            u'records': [hit.data.to_dict() for hit in result],
            u'facets': utils.format_facets(result.aggs.to_dict()),
            u'fields': fields,
            u'raw_fields': mapping[u'mappings'][DOC_TYPE][u'properties'][u'data'][u'properties'],
            u'after': utils.get_last_after(result),
            u'_backend': u'versioned-datastore',
        }


def datastore_create(context, data_dict):
    '''
    Adds a resource to the versioned datastore. This action doesn't take any data, it simply ensures
    any setup work is complete for the given resource in the search backend. To add data after
    creating a resource in the datastore, use the datastore_upsert action.

    :param resource_id: resource id of the resource
    :type resource_id: string

    **Results:**

    :returns: True if the datastore was initialised for this resource (or indeed if it was already
              initialised) and False if not. If False is returned this implies that the resource
              cannot be ingested into the datastore because the format is not supported
    :rtype: boolean
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_create_schema())
    toolkit.check_access(u'datastore_create', context, data_dict)

    resource_id = data_dict[u'resource_id']

    if utils.is_resource_read_only(resource_id):
        return False

    # lookup the resource dict
    resource = toolkit.get_action(u'resource_show')(context, {u'id': resource_id})
    # only create the index if the resource is ingestable
    if utils.is_ingestible(resource):
        # note that the version parameter doesn't matter when creating the index so we can safely
        # pass None
        utils.SEARCH_HELPER.ensure_index_exists(DatastoreIndex(utils.CONFIG, resource_id, None))
        # make sure the privacy is correctly setup
        utils.update_privacy(resource_id)
        return True
    return False


def datastore_upsert(context, data_dict):
    '''
    Upserts data into the datastore for the resource. The data can be provided in the data_dict
    using the key 'records' or, if data is not specified, the URL on the resource is used.

    :param resource_id: resource id of the resource
    :type resource_id: string
    :param replace: whether to remove any records not included in this update. If True, any record
                    that was not in the set of data ingested is marked as deleted. Note that this is
                    any record not included, not any record that hasn't changed. If a record is
                    included in the data, has an _id that matches an existing record and its data
                    hasn't changed, it will not be removed.
                    If False, the data is ingested and all existing data is left to be either
                    updated or continue to be current. There is no default for this option.
    :type replace: bool
    :param version: the version to store the data under (optional, if not specified defaults to now)
    :type version: int

    **Results:**

    :returns: details about the job that has been submitted to fulfill the upsert request.
    :rtype: dict

    '''
    # this comes through as junk if it's not removed before validating. This happens because the
    # data dict is flattened during validation, but why this happens is unclear.
    records = data_dict.get(u'records', None)
    data_dict = utils.validate(context, data_dict, schema.datastore_upsert_schema())
    toolkit.check_access(u'datastore_upsert', context, data_dict)

    resource_id = data_dict[u'resource_id']

    if utils.is_resource_read_only(resource_id):
        raise utils.ReadOnlyResourceException(u'This resource has been marked as read only')

    replace = data_dict[u'replace']
    # these 3 parameters are all optional and have the defaults defined below
    version = data_dict.get(u'version', to_timestamp(datetime.now()))

    # check that the version is valid
    if not check_version_is_valid(resource_id, version):
        raise utils.InvalidVersionException(u'The new version must be newer than current version')

    # get the current user
    user = toolkit.get_action(u'user_show')(context, {u'id': context[u'user']})

    # queue the resource import job
    resource = toolkit.get_action(u'resource_show')(context, {u'id': resource_id})
    job = queue_import(resource, version, replace, records, user[u'apikey'])

    return {
        u'queued_at': job.enqueued_at.isoformat(),
        u'job_id': job.id,
    }


def datastore_delete(context, data_dict):
    '''
    Deletes the data in the datastore against the given resource_id. Note that this is achieved by
    setting all records to be empty in a new version and then indexing that new version. This
    ensures that the data is not available in the latest version but is in old ones.

    :param resource_id: resource id of the resource
    :type resource_id: string
    :param version: the version to delete the data at, can be missing and if it is it's defaults to
                    the current timestamp
    :type version: integer
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_delete_schema())
    toolkit.check_access(u'datastore_delete', context, data_dict)

    resource_id = data_dict[u'resource_id']
    # get the requested deletion version, or default to now
    version = data_dict.get(u'version', to_timestamp(datetime.now()))

    if utils.is_resource_read_only(resource_id):
        raise toolkit.ValidationError(u'This resource has been marked as read only')

    # queue the job
    resource = toolkit.get_action(u'resource_show')(context, {u'id': resource_id})
    job = queue_deletion(resource, version)
    return {
        u'queued_at': job.enqueued_at.isoformat(),
        u'job_id': job.id,
    }


@toolkit.side_effect_free
def datastore_get_record_versions(context, data_dict):
    '''
    Given a record id and an resource it appears in, returns the version timestamps available for
    that record in ascending order.

    Data dict params:
    :param resource_id: resource id that the record id appears in
    :type resource_id: string
    :param id: the id of the record
    :type id: integer

    **Results:**

    :returns: a list of versions
    :rtype: list
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_get_record_versions_schema())
    index_name = utils.prefix_resource(data_dict[u'resource_id'])
    return utils.SEARCH_HELPER.get_record_versions(index_name, int(data_dict[u'id']))


@toolkit.side_effect_free
def datastore_get_resource_versions(context, data_dict):
    '''
    Given a resource id, returns the version timestamps available for that resource in ascending
    order along with the number of records modified in the version and the number of records at that
    version.

    This action also accepts all the datastore_search parameters that can be used to search the
    resource's data (i.e. q, filters). If these parameters are passed then the returned versions and
    counts are for the data found using the search. This allows the discovery of the versions and
    counts available for a query's result set.

    Data dict params:
    :param resource_id: resource id
    :type resource_id: string

    **Results:**

    :returns: a list of dicts, each in the form: {"version": #, "changes": #, "count": #}
    :rtype: list of dicts
    '''
    index_name = utils.prefix_resource(data_dict[u'resource_id'])

    original_data_dict, data_dict, version, search = create_search(context, data_dict)

    data = utils.SEARCH_HELPER.get_index_version_counts(index_name, search=search)

    search = search.using(utils.CLIENT).index(index_name)[0:0]
    for result in data:
        version = result[u'version']
        count = search.filter(create_version_query(version)).count()
        result[u'count'] = count
    return data


@toolkit.side_effect_free
def datastore_autocomplete(context, data_dict):
    '''
    Provides autocompletion results against a specific field in a specific resource.

    **Data dict params:**

    :param resource_id: id of the resource to be searched against
    :type resource_id: string
    :param q: full text query. If a string is passed, all fields are searched with the value. If a
          dict is passed each of the fields and values contained within will be searched as
          required (e.g. {"field1": "a", "field2": "b"}).
    :type q: string or dictionary
    :param filters: a dictionary of conditions that must be met to match a record
                    (e.g {"field1": "a", "field2": "b"}) (optional)
    :type filters: dictionary
    :param limit: maximum number of records to return (optional, default: 100)
    :type limit: int
    :param after: search after offset value as a base64 encoded string
    :type after: string
    :param field: the field to autocomplete against
    :type field: string
    :param term: the search term for the autocompletion
    :type term: string
    :param version: version to search at, if not provided the current version of the data is
                   searched.
    :type version: int, number of milliseconds (not seconds!) since UNIX epoch


    **Results:**

    :returns: a dict containing the list of values and an after value for the next page's results
    :rtype: dict
    '''
    # ensure the data dict is valid against our autocomplete action schema
    data_dict = utils.validate(context, data_dict, schema.datastore_autocomplete_schema())

    # extract the fields specifically needed for setting up the autocomplete query
    field = data_dict.pop(u'field')
    term = data_dict.pop(u'term')
    after = data_dict.pop(u'after', None)
    # default to a size of 20 results
    size = data_dict.pop(u'limit', 20)
    # ensure the search doesn't respond with any hits cause we don't need them
    data_dict[u'limit'] = 0
    # remove the offset if one was passed as we don't need it
    data_dict.pop(u'offset', None)

    # now build the search object against the normal search code
    _original_data_dict, data_dict, version, search = create_search(context, data_dict)
    # get the index we're going to search against
    index_name = utils.prefix_resource(data_dict[u'resource_id'])

    # add the autocompletion query part which takes the form of a prefix search
    search = search.filter(u'prefix', **{prefix_field(field): term})
    # modify the search so that it has the aggregation required to get the autocompletion results
    search.aggs.bucket(u'field_values', u'composite', size=size,
                       sources={field: A(u'terms', field=prefix_field(field), order=u'asc')})
    # if there's an after included, add it into the aggregation
    if after:
        search.aggs[u'field_values'].after = {field: after}

    # run the search (this adds the version to the query too)
    result = utils.run_search(search, [index_name], version)

    # get the results we're interested in
    agg_result = result.aggs.to_dict()[u'field_values']
    # return a dict of results, but only include the after details if there are any to include
    return_dict = {
        u'values': [bucket[u'key'][field] for bucket in agg_result[u'buckets']],
    }
    if u'after_key' in agg_result:
        return_dict[u'after'] = agg_result[u'after_key'][field]
    return return_dict


def datastore_reindex(context, data_dict):
    '''
    Triggers a reindex of the given resource's data. This does not reingest the data to mongo, but
    it does reindex the data in mongo to elasticsearch. The intent of this action is to allow
    mapping changes (for example) to be picked up.

    Data dict params:
    :param resource_id: resource id that the record id appears in
    :type resource_id: string

    **Results:**

    :returns: a dict containing the details of the reindex as returned from elasticsearch
    :rtype: dict
    '''
    # validate the data dict
    data_dict = utils.validate(context, data_dict, schema.datastore_reindex())
    # check auth
    toolkit.check_access(u'datastore_reindex', context, data_dict)
    # retrieve the resource id
    resource_id = data_dict[u'resource_id']

    if utils.is_resource_read_only(resource_id):
        raise toolkit.ValidationError(u'This resource has been marked as read only')

    last_ingested_version = stats.get_last_ingest(resource_id)
    if last_ingested_version is None:
        raise toolkit.ValidationError(u'There is no ingested data for this version')

    resource = toolkit.get_action(u'resource_show')(context, {u'id': resource_id})
    job = queue_index(resource, None, last_ingested_version.version)

    return {
        u'queued_at': job.enqueued_at.isoformat(),
        u'job_id': job.id,
    }


@toolkit.side_effect_free
def datastore_query_extent(context, data_dict):
    '''
    Return the geospatial extent of the results of a given datastore search query. The data_dict
    parameters are the same as the arguments for `datastore_search`.


    **Results:**

    :rtype: A dict with the following keys
    :param total_count: total number of rows matching the query
    :type fields: integer
    :param geom_count: Number of rows matching the query that have geospatial information
    :type geom_count: int
    :param bounds: the extent of the query's results, this will be missing if no bound can be
                   calculated (for example, if the resource has no geo data)
    :type bounds: list in the format [[lat min, long min], [lat max, long max]]
    '''
    # ensure the search doesn't respond with any hits cause we don't need them and override two
    # unused params
    data_dict[u'limit'] = 0
    data_dict.pop(u'offset', None)
    data_dict.pop(u'after', None)

    # now build the search object against the normal search code
    _original_data_dict, data_dict, version, search = create_search(context, data_dict)
    # get the index we're going to search against
    index_name = utils.prefix_resource(data_dict[u'resource_id'])

    # add our bounds and geo count aggregations
    search.aggs.bucket(u'bounds', u'geo_bounds', field=u'meta.geo', wrap_longitude=False)
    search.aggs.bucket(u'geo_count', u'value_count', field=u'meta.geo')

    # add version filter and run the search
    result = utils.run_search(search, [index_name], version)
    agg_result = result.aggs.to_dict()

    # create a dict of results for return
    to_return = {
        u'total_count': result.hits.total,
        u'geom_count': agg_result[u'geo_count'][u'value'],
    }

    # extract and add the bounds info from the aggregations if there is any
    if agg_result[u'geo_count'][u'value'] > 0:
        top_left = agg_result[u'bounds'][u'bounds'][u'top_left']
        bottom_right = agg_result[u'bounds'][u'bounds'][u'bottom_right']
        to_return[u'bounds'] = [[p[u'lat'], p[u'lon']] for p in (top_left, bottom_right)]

    return to_return


@toolkit.side_effect_free
def datastore_get_rounded_version(context, data_dict):
    '''
    Round the requested version of this query down to the nearest actual version of the
    resource. This is necessary because we work in a system where although you can just query at
    a timestamp you should round it down to the nearest known version. This guarantees that when
    you come back and search the data later you'll get the same data. If the version requested
    is older than the oldest version of the resource then the requested version itself is
    returned (this is just a choice I made, we could return 0 for example instead).

    An example: a version of resource A is created at t=2 and then a search is completed on it
    at t=5. If a new version is created at t=3 then the search at t=5 won't return the data it
    should. We could solve this problem in one of two ways:

        - limit new versions of resources to only be either older than the oldest saved search
        - rely on the current time when resource versions are created and searches are saved

    There are however issues with both of these approaches:

        - forcing new resource versions to exceed currently created search versions would
          require a reasonable amount of work to figure out what the latest search version is and
          also crosses extension boundaries as we'd need access to any tables that have a latest
          query version.
        - we want to be able to create resource versions beyond the latest version in the system
          but before the current time to accommodate non-live data (i.e. data that comes from
          timestamped dumps). There are a few benefits to allowing this, for example it allows
          loading data where we create a number of resource versions at the same time but the
          versions themselves represent when the data was extacted from another system or indeed
          created rather than when it was loaded into CKAN.

    Data dict params:
    :param resource_id: the resource id
    :type resource_id: string
    :param version: the version to round (optional, if missing the latest version is returned)
    :type version: integer

    **Results:**

    :returns: the rounded version or None if no versions are available for the given resource id
    :rtype: integer or None
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_get_rounded_version_schema())
    index_name = utils.prefix_resource(data_dict[u'resource_id'])
    version = data_dict.get(u'version', None)
    return utils.SEARCH_HELPER.get_rounded_versions([index_name], version)[index_name]


@toolkit.side_effect_free
def datastore_search_raw(context, data_dict):
    '''
    This action allows you to search data in a resource using a raw elasticsearch query. This action
    allows more flexibility over the search both in terms of querying using any of elasticsearch's
    different DSL queries as well as aspects like turning versioning on and off.

    If the resource to be searched is private then appropriate authorization is required.

    Note that the structure of the documents in elasticsearch is defined as such:

        - data._id: the id of the record, this is always an integer
        - data.*: the data fields. Each field is stored in 3 different ways:
            - data.<field_name>: keyword type
            - data.<field_name>.full: text type
            - data.<field_name>.number: double type, will be missing if the data value isn't
                                        convertable to a number
        - meta.*: various metadata fields, including:
            - meta.all: a text type field populated from all the data in the data.* fields
            - meta.geo: if a pair of lat/lon fields have been assigned on this resource this geo
                        point type field is available
            - meta.version: the version of this record, this field is a date type field represented
                            in epoch millis
            - meta.next_version: the next version of this record, this field is a date type field
                            represented in epoch millis. If missing this is the current version of
                            the record
            - meta.versions: a date range type field which encapsulates the version range this
                             document applies to for the record

    Params:

    :param resource_id: id of the resource to be searched against, required
    :type resource_id: string
    :param search: the search JSON to submit to elasticsearch. This should be a valid elasticsearch
                   search, the only modifications that will be made to it are setting the version
                   filter unless include_version=False. If not included then an empty {} is used.
    :type search: dict
    :param version: version to search at, if not provided the current version of the data is
                   searched (unless using include_version=False).
    :type version: int, number of milliseconds (not seconds!) since UNIX epoch
    :param raw_result: whether to parse the result and return in the same format as datastore_search
                       or just return the exact elasticsearch response, unaltered. Defaults to False
    :type raw_result: bool
    :param include_version: whether to include the version filter or not. By default this is True.
                            This can only be set to False in combination with raw_result=True.
    :type include_version: bool


    **Results:**

    The result of this action is a dictionary with the following keys:

    :rtype: A dict with the following keys
    :param fields: fields/columns and their extra metadata
    :type fields: list of dicts
    :param total: number of total matching records
    :type total: int
    :param records: list of matching results
    :type records: list of dicts
    :param facets: list of fields and their top 10 values, if requested
    :type facets: dict
    :param after: the next page's search_after value which can be passed back as the "after"
                  parameter. This value will always be included if there were results otherwise None
                  is returned. A value will also always be returned even if this page is the last.
    :type after: a list or None

    In addition to returning this result, the actual result object is made available through the
    context dict under the key "versioned_datastore_query_result". This isn't available through the
    http action API however.

    If raw_result is True, then the elasticsearch response is returned without modification.
    '''
    # create a copy of the data dict for plugins later
    original_data_dict = copy.deepcopy(data_dict)
    # validate the data dict
    data_dict = utils.validate(context, data_dict, schema.datastore_search_raw_schema())

    # pull out the parameters
    resource_id = data_dict[u'resource_id']
    search = data_dict.get(u'search', {})
    version = data_dict.get(u'version', to_timestamp(datetime.now()))
    raw_result = data_dict.get(u'raw_result', False)
    include_version = data_dict.get(u'include_version', True)

    # interpret the parameters
    index_name = utils.prefix_resource(resource_id)
    search = Search.from_dict(search)

    try:
        # the user has asked for a raw result and that the version filter is not included
        if raw_result and not include_version:
            version = None

        # run the query passing the version which will either be the requested version, the current
        # timestamp or None if no version filter should be included in the search
        result = utils.run_search(search, index_name, version)

        if raw_result:
            return result.to_dict()

        # allow other extensions implementing our interface to modify the result object
        for plugin in PluginImplementations(IVersionedDatastore):
            result = plugin.datastore_modify_result(context, original_data_dict, data_dict, result)

        # add the actual result object to the context in case the caller is an extension and
        # they have used one of the interface hooks to alter the search object and include, for
        # example, an aggregation
        context[u'versioned_datastore_query_result'] = result

        # get the fields
        mapping, fields = utils.get_fields(resource_id, version)
        # allow other extensions implementing our interface to modify the field definitions
        for plugin in PluginImplementations(IVersionedDatastore):
            fields = plugin.datastore_modify_fields(resource_id, mapping, fields)

        # return a dictionary containing the results and other details
        return {
            u'total': result.hits.total,
            u'records': [hit.data.to_dict() for hit in result],
            u'facets': utils.format_facets(result.aggs.to_dict()),
            u'fields': fields,
            u'raw_fields': mapping[u'mappings'][DOC_TYPE][u'properties'][u'data'][u'properties'],
            u'after': utils.get_last_after(result),
            u'_backend': u'versioned-datastore',
        }
    except RequestError as e:
        raise toolkit.ValidationError(str(e))


def datastore_ensure_privacy(context, data_dict):
    '''
    Ensure that the privacy settings are correct across all resources in the datastore or for just
    one resource.

    Params:
    :param resource_id: optionally, the id of a specific resource to update. If this is present then
                        only the resource provided will have it's privacy updated. If it is not
                        present, all resources are updated.
    :type resource_id: string


    **Results:**
    The result of this action is a dictionary with the following keys:

    :rtype: A dict with the following keys
    :param modified: the number of resources that had their privacy setting modified
    :type ensured: integer
    :param total: the total number of resources examined
    :type total: integer
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_ensure_privacy_schema())

    modified = 0
    total = 0
    if u'resource_id' in data_dict:
        # just do this one
        utils.update_privacy(data_dict[u'resource_id'])
        ensured = 1
    else:
        package_data_dict = {u'limit': 50, u'offset': 0}
        while True:
            # iteratively retrieve all packages and ensure their resources
            packages = toolkit.get_action(u'current_package_list_with_resources')(context,
                                                                                  package_data_dict)
            if not packages:
                # we've ensured all the packages that are available
                break
            else:
                package_data_dict[u'offset'] += len(packages)
                for package in packages:
                    for resource in package.get(u'resources', []):
                        if resource[u'datastore_active']:
                            total += 1
                            if utils.update_privacy(resource[u'id'], package[u'private']):
                                modified += 1

    return {u'modified': modified, u'total': total}


@toolkit.side_effect_free
def datastore_multisearch(context, data_dict):
    '''
    This action allows you to search data in multiple resources.

    As well as returning the results of the search, this action also returns a slug which can be
    used to retrieve the query (not the query results) at a later time. This slug will return, if
    resolved using the datastore_resolve_slug action) the query, the query version, the resource ids
    and the version the query was run against. A few important notes:

        - the slug returned will always be unique even for the same search parameter combinations

        - the slugs can be reused after they expire

        - there is a small chance the returned slug will be the same as one already created, but
          this does not effect the underlying query the slug points to, it simply means that the
          slug may last longer than the default ttl

        - if the version parameter is missing or null/None, the resolved slug will reflect this and
          not provide a version value

        - the resource ids associated with the slug will always be the list of resources actually
          searched by the query, not just the list of resources passed as the resources parameter

    Params:

    :param query: the search JSON
    :type query: dict
    :param version: version to search at, if not provided the current version of the data is
                   searched
    :type version: int, number of milliseconds (not seconds!) since UNIX epoch
    :param query_version: the query language version (for example v1.0.0)
    :type query_version: string
    :param resource_ids: a list of resource ids to search. If no resources ids are specified (either
                         because the parameter is missing or because an empty list is passed) then
                         all resources in the datastore that the user can access are searched. Any
                         resources that the user cannot access or that aren't datastore resources
                         are skipped. If this means that no resources are available from the
                         provided list then a ValidationError is raised.
    :type resource_ids: a list of strings
    :param after: provides pagination. By passing a previous result set's after value, the next
                  page's results can be found. If not provided then the first page is retrieved
    :type after: a list
    :param size: the number of records to return in the search result. If not provided then the
                 default value of 100 is used. This value must be between 0 and 1000 and will be
                 capped at which ever end is necessary if it is beyond these bounds
    :type size: int
    :param top_resources: whether to include the top 10 resources in the query result, default False
    :type top_resources: bool
    :param pretty_slug: whether to return a pretty slug with words or a uuid. Default: True
    :type pretty_slug: bool

    **Results:**

    The result of this action is a dictionary with the following keys:

    :rtype: A dict with the following keys
    :param total: number of total matching records
    :type total: int
    :param records: list of matching results as dicts. Each dict will contain a "data" key which
                    holds the record data, and a "resource" key which holds the resource id the
                    record belongs to.
    :type records: list of dicts
    :param after: the next page's search_after value which can be passed back in the "after"
                  parameter. This value will always be included if there were results otherwise None
                  is returned. A value will also always be returned even if this page is the last.
    :type after: a list or None
    :param top_resources: if requested, the top 10 resources and the number of records matched in
                          them by the query
    :type top_resources: list of dicts
    :param skipped_resources: a list of the resources from the requested resource_ids parameter that
                              were not searched. This list is only populated if a the resource ids
                              paramater is passed, otherwise it will be an empty list. The resources
                              in this list will have been skipped for one of two reasons: either the
                              user doesn't have access to the requested resource or the requested
                              resource isn't a datastore resource.
    :type skipped_resources: a list of strings
    '''
    # TODO: allow specifying the version to search at per resource
    # TODO: should we return field info? If so how?
    data_dict = utils.validate(context, data_dict, schema.datastore_multisearch_schema())

    # extract the parameters
    query = data_dict.get(u'query', {})
    # the query version defaults to the latest available version
    query_version = data_dict.get(u'query_version', get_latest_query_version())
    # the version, if not passed then this will be defaulted to the current epoch time later
    version = data_dict.get(u'version', None)
    # the requested resources defaults to all of them (an empty list)
    requested_resource_ids = data_dict.get(u'resource_ids', [])
    # the after parameter if there is one
    after = data_dict.get(u'after', None)
    # the size parameter if there is one, if not default to 100. The size must be between 0 and 1000
    size = max(0, min(data_dict.get(u'size', 100), 1000))
    # the top_resources parameter if there is one
    top_resources = data_dict.get(u'top_resources', False)
    # whether to create a pretty slug or not
    pretty_slug = data_dict.get(u'pretty_slug', True)

    # figure out which resources should be searched
    resource_ids = utils.get_available_datastore_resources(context, requested_resource_ids)
    if not resource_ids:
        raise toolkit.ValidationError(u"The requested resources aren't accessible to this user")

    try:
        search, slug = create_search_and_slug(query, query_version, version, resource_ids,
                                              pretty_slug=pretty_slug)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    # add a simple default sort to ensure we get an after value for pagination
    search = search.sort({u'data._id': u'desc'})
    # add the after if there is one
    if after is not None:
        search = search.extra(search_after=after)
    # add the size parameter
    search = search.extra(size=size)

    if top_resources:
        # gather the number of hits in the top 10 most frequently represented indexes if requested
        search.aggs.bucket(u'indexes', u'terms', field=u'_index')

    # create a multisearch for this one query - this ensures there aren't any issues with the length
    # of the URL as the index list is passed as a part of the body
    multisearch = MultiSearch(using=utils.CLIENT).add(search)
    # run the search and get the only result from the search results list
    result = next(iter(multisearch.execute()))

    response = {
        u'total': result.hits.total,
        u'after': utils.get_last_after(result),
        u'records': [{
            u'data': hit.data.to_dict(),
            # should we provide the name too? If so cache a map of id -> name, then update it if we
            # don't find the id in the map
            u'resource': utils.trim_index_name(hit.meta.index),
        } for hit in result.hits],
        # note that resource_ids is a set and therefore the in check is speedy
        u'skipped_resources': [rid for rid in requested_resource_ids if rid not in resource_ids],
        u'slug': slug,
    }

    if top_resources:
        # include the top resources if requested
        response[u'top_resources'] = [
            {utils.trim_index_name(bucket[u'key']): bucket[u'doc_count']}
            for bucket in result.aggs.to_dict()[u'indexes'][u'buckets']
        ]

    return response


def datastore_create_slug(context, data_dict):
    '''
    Create a query slug based on the provided query parameters.

    This action returns a slug which can be used to retrieve the query parameters passed (not the
    query results) at a later time. This slug will return, if resolved using the
    datastore_resolve_slug action) the query, the query version, the resource ids and the version
    the query was run against. A few important notes:

        - the slug returned will always be unique even for the same search parameter combinations

        - the slugs can be reused after they expire

        - there is a small chance the returned slug will be the same as one already created, but
          this does not effect the underlying query the slug points to, it simply means that the
          slug may last longer than the default ttl

        - if the version parameter is missing or null/None, the resolved slug will reflect this and
          not provide a version value

        - the resource ids associated with the slug will always be the list of resources actually
          searched by the query, not just the list of resources passed as the resources parameter

    Params:

    :param query: the search JSON
    :type query: dict
    :param version: version to search at, if this value is null or missing then the slug will not
                    include version information
    :type version: int, number of milliseconds (not seconds!) since UNIX epoch
    :param query_version: the query language version (for example v1.0.0)
    :type query_version: string
    :param resource_ids: a list of resource ids to search. If no resources ids are specified (either
                         because the parameter is missing or because an empty list is passed) then
                         all resources in the datastore that the user can access are searched. Any
                         resources that the user cannot access or that aren't datastore resources
                         are skipped. If this means that no resources are available from the
                         provided list then a ValidationError is raised.
    :type resource_ids: a list of strings
    :param pretty_slug: whether to return a pretty slug with words or a uuid. Default: True
    :type pretty_slug: bool
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_create_slug())

    # extract the parameters
    query = data_dict.get(u'query', {})
    # the query version defaults to the latest available version
    query_version = data_dict.get(u'query_version', get_latest_query_version())
    # the version defaults to the current time
    version = data_dict.get(u'version', None)
    # the requested resources defaults to all of them (an empty list)
    requested_resource_ids = data_dict.get(u'resource_ids', [])
    # whether to create a pretty slug or not
    pretty_slug = data_dict.get(u'pretty_slug', True)

    # figure out which resources should be searched
    resource_ids = utils.get_available_datastore_resources(context, requested_resource_ids)
    if not resource_ids:
        raise toolkit.ValidationError(u"The requested resources aren't accessible to this user")

    try:
        _search, slug = create_search_and_slug(query, query_version, version, resource_ids,
                                               pretty_slug=pretty_slug)
    except (jsonschema.ValidationError, InvalidQuerySchemaVersionError) as e:
        raise toolkit.ValidationError(e.message)

    return slug


@toolkit.side_effect_free
def datastore_resolve_slug(context, data_dict):
    '''
    Given a slug, resolves it and returns the query information associated with it.

    Params:

    :param slug: the slug to resolve
    :type slug: string

    **Results:**

    The result of this action is a dictionary with the following keys:

    :rtype: A dict with the following keys
    :param query: the query body
    :type query: dict
    :param query_version: the query version in use
    :type query_version: string
    :param version: the version the query is using
    :type version: integer
    :param resource_ids: the resource ids under search in the query
    :type resource_ids: a list of strings
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_resolve_slug())
    slug_info = resolve_slug(data_dict[u'slug'])
    if slug_info is None:
        raise toolkit.ValidationError(u'Slug not found')

    # only return the query, query_version, version and resource_ids
    return {k: slug_info[k] for k in (u'query', u'query_version', u'version', u'resource_ids')}


@toolkit.side_effect_free
def datastore_field_autocomplete(context, data_dict):
    '''
    Returns a dictionary of available fields in the datastore which match the passed prefix. The
    fields will be retrieved from the given public resources or all public resources if no
    resource_ids option is provided.

    Params:

    :param prefix: prefix to match the fields against. Optional, by default all fields are matched
    :type prefix: string
    :param resource_ids: the resources to find the fields in. Optional, by default all public
                         resources are used
    :type version: list of string resource ids, separated by commas
    :param lowercase: whether to do a case insensitive prefix match. Optional, default: False

    **Results:**

    The result of this action is a dictionary with the following keys:

    :rtype: A dict with the following keys
    :param fields: a dict of field names and their field properties. Example:
                   {
                       "field1": {
                           "type": "keyword",
                           "fields": {
                               "full": "text",
                               "number": "double"
                           }
                       },
                   }
                   The type value is the index type of the field if accessed directly. The fields
                   value indicates the presence of subfields with their names and associated index
                   types included.
    :type fields: dict
    :param count: the number of fields returned
    :type count: int
    '''
    data_dict = utils.validate(context, data_dict, schema.datastore_field_autocomplete_schema())

    # TODO: support case sensitive prefixing, maybe?
    # TODO: allow choice of prefix searching on nested field name as a whole or as parts
    prefix = data_dict.get(u'prefix', u'')
    resource_ids = data_dict.get(u'resource_ids', [])
    lowercase = data_dict.get(u'lowercase', False)

    if len(resource_ids) == 0:
        # get all public index mappings
        target = utils.get_public_alias_name(u'*')
    else:
        # just get the public index mappings for the requested resource ids
        target = u','.join(map(utils.get_public_alias_name, resource_ids))

    fields = defaultdict(dict)
    mappings = utils.CLIENT.indices.get_mapping(target)
    for index, mapping in mappings.items():
        resource_id = utils.unprefix_index(index)

        for field_path, config in utils.iter_data_fields(mapping):
            if any((part.lower() if lowercase else part).startswith(prefix) for part in field_path):
                fields[u'.'.join(field_path)][resource_id] = {
                    u'type': config[u'type'],
                    u'fields': {f: c[u'type'] for f, c in config.get(u'fields', {}).items()}
                }

    return {
        u'count': len(fields),
        u'fields': fields,
    }
