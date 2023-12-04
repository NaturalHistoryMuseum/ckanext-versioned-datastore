datastore_search = '''
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

datastore_create = '''
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

datastore_upsert = '''
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

datastore_delete = '''
Deletes the data in the datastore against the given resource_id. Note that this is achieved by
setting all records to be empty in a new version and then indexing that new version. This
ensures that the data is not available in the latest version but is in old ones.

:param resource_id: resource id of the resource
:type resource_id: string
:param version: the version to delete the data at, can be missing and if it is it's defaults to
                the current timestamp
:type version: integer
'''

datastore_reindex = '''
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

datastore_ensure_privacy = '''
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

datastore_autocomplete = '''
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

datastore_query_extent = '''
Return the geospatial extent of the results of a given datastore search query. The data_dict
parameters are the same as the arguments for `datastore_search`.


**Results:**

:rtype: A dict with the following keys
:param total_count: total number of rows matching the query
:type total_count: integer
:param geom_count: Number of rows matching the query that have geospatial information
:type geom_count: int
:param bounds: the extent of the query's results, this will be missing if no bound can be
               calculated (for example, if the resource has no geo data)
:type bounds: list in the format [[lat min, long min], [lat max, long max]]
'''

datastore_search_raw = '''
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

datastore_get_record_versions = '''
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

datastore_get_resource_versions = '''
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

datastore_get_rounded_version = '''
Round the requested version of this query down to the nearest actual version of the
resource. This is necessary because we work in a system where although you can just query at
a timestamp you should round it down to the nearest known version. This guarantees that when
you come back and search the data later yo'll get the same data. If the version requested
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

datastore_multisearch = '''
This action allows you to search data in multiple resources.

The resources that are searched for the in this action and the version they are searched at are
both extracted from the resource_ids_and_versions in the first instance, and if no information
in there is found then the resource_ids and version parameters are used as fall backs.
Regardless of where the resource ids list comes from though, it is always checked against
permissions to ensure the user has the right to access the given resources. Any resources
included that the user doesn't have access to are not searched and will be returned in the
"skipped_resources" part of the return value.


Params:

:param query: the search JSON
:type query: dict
:param version: version to search at, if not provided the current version of the data is
               searched
:type version: int, number of milliseconds (not seconds!) since UNIX epoch
:param resource_ids_and_versions: a dict of resource ids and the versions to search them at. If
                                  this is present it's values are prioritised over the version
                                  and resource_ids parameters.
:type resource_ids_and_versions: dict of strings -> ints (number of milliseconds (not seconds!)
                                 since UNIX epoch)
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
:param timings: whether to include timings in the result dict, default False. This is intended for
                developer debugging.
:type timings: bool

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
:param timings: dict of events and how long they took as part of the response processing. This is
                only included in the response if the timings parameter is True
:type timings: dict
'''

datastore_create_slug = '''
Create a query slug based on the provided query parameters.

This action returns a slug which can be used to retrieve the query parameters passed (not the
query results) at a later time. This slug will return, if resolved using the
datastore_resolve_slug action, the query, the query version, the resource ids, the version and
the specific resources/versions map passed in to this action to create the slug. A few important
notes:

    - the slug returned will always be the same for the same search parameter combinations

    - the slugs never expire

    - if the version parameter is missing or null/None, the resolved slug will reflect this and
      not provide a version value - essentially we don't default it to the current time if it's
      missing


Params:

:param query: the search JSON
:type query: dict
:param version: version to search at, if this value is null or missing then the slug will not
                include version information
:type version: int, number of milliseconds (not seconds!) since UNIX epoch
:param query_version: the query language version (for example v1.0.0)
:type query_version: string
:param resource_ids_and_versions: a dict of resource ids and the versions to search them at. If
                                  this is present it's values are prioritised over the version
                                  and resource_ids parameters.
:type resource_ids_and_versions: dict of strings -> ints (number of milliseconds (not seconds!)
                                 since UNIX epoch)
:param resource_ids: a list of resource ids to search
:type resource_ids: a list of strings
:param pretty_slug: whether to return a pretty slug with words or a uuid. Default: True
:type pretty_slug: bool

**Results:**

The result of this action is a dictionary with the following keys:

:rtype: A dict with the following keys
:param slug: the slug
:type slug: string
:param is_new: whether the returned slug was newly created or already existed
:type is_new: bool
'''

datastore_resolve_slug = '''
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
:param resource_ids_and_versions: a dict of resource ids -> versions to search them at
:type resource_ids_and_versions: a dict
:param created: the date time the slug was originally created
:type created: datetime in isoformat
'''

datastore_field_autocomplete = '''
Returns a dictionary of available fields in the datastore which contain the passed text. The
fields will be retrieved from resources available to the user. If a list of resource ids is
passed as a parameter then the resources from that list that the user has access to will be
used.

Params:

:param text: prefix to match the fields against. Optional, by default all fields are matched
:type text: string
:param resource_ids: the resources to find the fields in. Optional, by default all resources
                     available to the user are used
:type resource_ids: list of string resource ids, separated by commas
:param lowercase: whether to do a case insensitive prefix match. Optional, default: False
:type lowercase: bool

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

datastore_value_autocomplete = '''
Finds values in the given field, from the given resources, which start with the given prefix and
returns up to {size} of them in a list. The values are sorted in alphabetical order.

Params:

:param field: the name of the field to autocomplete
:type field: str
:param prefix: the value to use as the prefix search, can be missing or blank
:type prefix: str
:param query: the search JSON
:type query: dict
:param version: version to search at, if not provided the current version of the data is
               searched
:type version: int, number of milliseconds (not seconds!) since UNIX epoch
:param resource_ids_and_versions: a dict of resource ids and the versions to search them at. If
                                  this is present it's values are prioritised over the version
                                  and resource_ids parameters.
:type resource_ids_and_versions: dict of strings -> ints (number of milliseconds (not seconds!)
                                 since UNIX epoch)
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
             default value of 20 is used. This value must be between 1 and 20 and will be capped at
             which ever end as necessary if it is beyond these bounds
:type size: int

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
:param timings: dict of events and how long they took as part of the response processing. This is
                only included in the response if the timings parameter is True
:type timings: dict

'''

datastore_queue_download = '''
Queues a task to generate a downloadable zip containing the data produced by the given query.

**Results:**

:returns: details about the job that has been submitted to fulfill the upsert request.
:rtype: dict
'''

datastore_regenerate_download = '''
Calls datastore_queue_download to regenerate a previous request. Note that notifier args
are still required as these are not stored in the original request, and server args may
be specified to override any stored ones.

**Results:**

:returns: details about the job that has been submitted to fulfill the upsert request.
:rtype: dict
'''

datastore_guess_fields = '''
This action allows you to retrieve a set of fields to display by default for a given search across
potentially multiple resources. The returned list of groups of fields is ordered by the number of
resources the fields in each group appears in under the provided query. Ties are handled by ordering
the group with the fields that appear in the most records first. If there is only one resource id
passed then the groups returned are ordered in ingestion order, if available.

The resources that are searched in this action and the version they are searched at are both
extracted from the resource_ids_and_versions in the first instance, and if no information in there
is found then the resource_ids and version parameters are used as fall backs. Regardless of where
the resource ids list comes from though, it is always checked against permissions to ensure the user
has the right to access the given resources.

Params:

:param query: the search JSON
:type query: dict
:param query_version: the query language version (for example v1.0.0)
:type query_version: string
:param version: version to search at, if not provided the current version of the data is searched
:type version: int, number of milliseconds (not seconds!) since UNIX epoch
:param resource_ids_and_versions: a dict of resource ids and the versions to search them at. If this
                                  is present it's values are prioritised over the version and
                                  resource_ids parameters.
:type resource_ids_and_versions: dict of strings -> ints (number of milliseconds (not seconds!)
                                 since UNIX epoch)
:param resource_ids: a list of resource ids to search. If no resources ids are specified (either
                     because the parameter is missing or because an empty list is passed) then
                     all resources in the datastore that the user can access are searched. Any
                     resources that the user cannot access or that aren't datastore resources
                     are skipped. If this means that no resources are available from the
                     provided list then a ValidationError is raised.
:type resource_ids: a list of strings
:param size: the number of field groups to return in the search result. If not provided then the
             default is 10. Each group can have many fields in it from many resources. Each group is
             created by finding common fields across resources - fields are matched by comparing
             them case-insensitively.
:type size: int
:param ignore_groups: names of groups to ignore from the returned list. Note that this is groups,
                      not fields!
:type ignore_groups: a list of strings

**Results:**

The result of this action is a list of dicts, each with the following keys:

:rtype: A list of dicts with the following keys
:param group: the name of the group
:type group: string
:param count: the number of resources including fields in the group
:type count: int
:params records: the number of records the group's fields appear in
:type records: int
:param fields: a dict of field names -> list of resource ids representing the fields in the group
               and the resources they come from
:type fields: dict
'''

datastore_hash_query = '''
This action simply hashes the given query and returns the hex digest of it. The hash is produced
using sha1 and a custom algorithm - it's not just a hash of the query dict.

Params:

:param query: the search JSON
:type query: dict
:param query_version: the query language version (for example v1.0.0)
:type query_version: string

Returns:
:rtype: string
'''

datastore_is_datastore_resource = '''
This action checks whether the given resource is in the datastore.

Params:

:param resource_id: the resource id to check
:type resource_id: string

Returns:

:rtype: boolean
'''

datastore_get_latest_query_schema_version = '''
This action simply returns the latest available query schema version.

Returns:

:rtype: string
'''

datastore_count = '''
Count the number of records available at a specific version across a set of resources. This
allows quick counting of total records without any query, if you want to count with a query,
use the search actions with a limit of 0.

Params:

:param resource_ids: optionally, the ids of specific resources to count. Only public resources
                     can be counted. If this parameter is not provided, then all public
                     resources are counted.
:type resource_id: string, list of comma separated strings
:param version: version to count at, if not provided the current timestamp is used
:type version: int, number of milliseconds (not seconds!) since UNIX epoch

**Results:**

The result of this action is a dictionary with the following keys:
:rtype: an integer count
'''

datastore_edit_slug = '''
Add or modify the reserved slug for a query. Reserved slugs can only be replaced by sysadmins, but
if one has not been added yet for a query, any logged-in user can supply it.

Params:

:param current_slug: the slug currently referencing the search (reserved or non-reserved)
:type current_slug: str
:param new_reserved_slug: the string to use as the new reserved slug
:type new_reserved_slug: str

Returns:

:rtype: bool
'''

datastore_multisearch_counts = """
Count the number of records that match the query in each of the provided resources and
return the counts for each resource in a dict.

The query and resource IDs are parsed in the same way as the datastore_multisearch
action.

Params:

:param query: the search JSON
:type query: dict
:param version: version to search at, if not provided the current version of the data is
               searched
:type version: int, number of milliseconds (not seconds!) since UNIX epoch
:param resource_ids_and_versions: a dict of resource ids and the versions to search them
                                  at. If this is present it's values are prioritised
                                  over the version and resource_ids parameters.
:type resource_ids_and_versions: dict of strings -> ints (number of milliseconds (not
                                 seconds!) since UNIX epoch)
:param query_version: the query language version (for example v1.0.0)
:type query_version: string
:param resource_ids: a list of resource ids to search. If no resources ids are specified
                     (either because the parameter is missing or because an empty list
                     is passed) then all resources in the datastore that the user can
                     access are searched. Any resources that the user cannot access or
                     that aren't datastore resources are skipped. If this means that no
                     resources are available from the provided list then a
                     ValidationError is raised.
:type resource_ids: a list of strings

**Results:**

The result of this action is a dictionary where the keys are the resource IDs and the
values are the number of records in that resource which matched the query.

:rtype: dict with str keys and int values
"""
