# -*- coding: utf-8 -*-
import copy
import logging

from datetime import datetime
from eevee.utils import to_timestamp
from eevee.indexing.utils import DOC_TYPE
from elasticsearch import NotFoundError, RequestError
from elasticsearch_dsl import A, Search

from ckan.plugins import toolkit, PluginImplementations
from ckan.lib.search import SearchIndexError
from ckanext.versioned_datastore.interfaces import IVersionedDatastore
from ckanext.versioned_datastore.lib import utils, stats
from ckanext.versioned_datastore.lib.importing import check_version_is_valid
from ckanext.versioned_datastore.lib.indexing.indexing import DatastoreIndex
from ckanext.versioned_datastore.lib.queuing import queue_index, queue_import, queue_deletion
from ckanext.versioned_datastore.lib.search import create_search, prefix_field
from ckanext.versioned_datastore.logic import schema

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

    # if the run query option is false (default to true if not present) then just return the query
    # we would have run against elasticsearch instead of actually running it. This is useful for
    # running the query outside of ckan, for example on a tile server.
    if not data_dict.get(u'run_query', True):
        # call pre_search to add all the versioning filters necessary (and other things too)
        result = utils.SEARCHER.pre_search(indexes=[index_name], search=search, version=version)
        return {
            # the first part of the pre_search response is a list of indexes to run the query
            # against
            u'indexes': result[0],
            # the second part is the search object itself which we can call to_dict on to pull the
            # query out
            u'search': result[1].to_dict(),
        }
    else:
        try:
            # run the search through eevee. Note that we pass the indexes to eevee as a list as
            # eevee is ready for cross-resource search but this code isn't (yet)
            result = utils.SEARCHER.search(indexes=[index_name], search=search, version=version)
        except NotFoundError as e:
            raise SearchIndexError(e.error)

        # allow other extensions implementing our interface to modify the result object
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
            u'total': result.total,
            u'records': [hit.data for hit in result.results()],
            u'facets': utils.format_facets(result.aggregations),
            u'fields': fields,
            u'raw_fields': mapping[u'mappings'][DOC_TYPE][u'properties'][u'data'][u'properties'],
            u'after': result.last_after,
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
        utils.SEARCHER.ensure_index_exists(DatastoreIndex(utils.CONFIG, resource_id, None))
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
        raise toolkit.ValidationError(u'This resource has been marked as read only')

    replace = data_dict[u'replace']
    # these 3 parameters are all optional and have the defaults defined below
    version = data_dict.get(u'version', to_timestamp(datetime.now()))

    # check that the version is valid
    if not check_version_is_valid(resource_id, version):
        raise toolkit.ValidationError(u'The new version must be newer than current version')

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
    return utils.SEARCHER.get_record_versions(index_name, int(data_dict[u'id']))


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

    data = utils.SEARCHER.get_index_version_counts(index_name, search=search)

    search = search.using(utils.SEARCHER.elasticsearch).index(index_name)[0:0]
    for result in data:
        version = result[u'version']
        count = search.filter(u'term', **{u'meta.versions': version}).count()
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

    # run the search
    result = utils.SEARCHER.search(indexes=[index_name], search=search, version=version)
    # get the results we're interested in
    agg_result = result.aggregations[u'field_values']
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

    # run the search
    result = utils.SEARCHER.search(indexes=[index_name], search=search, version=version)

    # create a dict of results for return
    to_return = {
        u'total_count': result.hits.total,
        u'geom_count': result.aggregations[u'geo_count'][u'value'],
    }

    # extract and add the bounds info from the aggregations if there is any
    if result.aggregations[u'geo_count'][u'value'] > 0:
        top_left = result.aggregations[u'bounds'][u'bounds'][u'top_left']
        bottom_right = result.aggregations[u'bounds'][u'bounds'][u'bottom_right']
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
    return utils.SEARCHER.get_rounded_versions([index_name], version)[index_name]


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
    version = data_dict.get(u'version', None)
    raw_result = data_dict.get(u'raw_result', False)
    include_version = data_dict.get(u'include_version', True)

    # interpret the parameters
    index_name = utils.prefix_resource(resource_id)
    search = Search.from_dict(search)

    try:
        if raw_result:
            # the caller doesn't want us to parse the response and return it in our format, they
            # just want the pure elasticsearch response.
            if include_version:
                # run pre search to get the search setup (this adds the version filter)
                _, search, _ = utils.SEARCHER.pre_search(indexes=[index_name], search=search,
                                                         version=version)
            else:
                # if we're not passing the search to the pre search function, we need to add the
                # index and client manually
                search = search.index(index_name).using(utils.SEARCHER.elasticsearch)

            # run it and just return the result directly
            return search.execute().to_dict()
        else:
            # run the search through eevee. Note that we pass the indexes to eevee as a list as
            # eevee is ready for cross-resource search but this code isn't (yet)
            result = utils.SEARCHER.search(indexes=[index_name], search=search, version=version)

            # allow other extensions implementing our interface to modify the result object
            for plugin in PluginImplementations(IVersionedDatastore):
                result = plugin.datastore_modify_result(context, original_data_dict, data_dict,
                                                        result)

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
                u'total': result.total,
                u'records': [hit.data for hit in result.results()],
                u'facets': utils.format_facets(result.aggregations),
                u'fields': fields,
                u'raw_fields': mapping[u'mappings'][DOC_TYPE][u'properties'][u'data'][
                    u'properties'],
                u'after': result.last_after,
                u'_backend': u'versioned-datastore',
            }
    except RequestError as e:
        raise toolkit.ValidationError(str(e))
    except NotFoundError as e:
        raise SearchIndexError(e.error)
