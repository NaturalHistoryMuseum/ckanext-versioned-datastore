vds_basic_query = """
This action allows you to search data in a resource. It is designed to function in a
similar way to CKAN's core datastore_search but with a few extra bells and whistles,
most prominently versioning. This allows the resource to be searched at any moment in
it's lifespan and have the data as it looked at that moment returned, even if it has
changed since.

If the resource to be searched is private then appropriate authorization is required.

Note that in the parameters listed below spaces should only be included if part of a
field name, so, for example, don't include any spaces in comma separated lists unless
needed.

:param resource_id: ID of the resource to be searched against
:type resource_id: string
:param q: full text query. If a string is passed, all fields are searched with the
          value. If a dict is passed each of the fields and values contained within will
          be searched as required (e.g. {"field1": "a", "field2": "b"}).
:type q: string or dictionary
:param filters: a dictionary of conditions that must be met to match a record
                (e.g {"field1": "a", "field2": "b"}) (optional)
:type filters: dictionary
:param after: search_after value for elasticsearch to paginate from (optional). Use this
              mechanism to do deep (beyond 10000 values) pagination. The values have to
              match the sort currently in use and therefore it's recommended that this
              value is not built but rather passed from the previous result's 'after'
              key.
:type after: a list of values
:param limit: maximum number of records to return (optional, default: 100)
:type limit: int
:param offset: offset this number of records (optional)
:type offset: int
:param fields: fields to return for each record (optional, default: all fields are
               returned)
:type fields: list or comma separated string
:param sort: list of field names with ordering. Ordering is ascending by default, if
             descending is required, add "desc" after the field name
             e.g.: "fieldname1,fieldname2 desc" sorts by fieldname1 asc and fieldname2
             desc
:type sort: list or comma separated string
:param version: version to search at, if not provided the current version of the data is
               searched.
:type version: int, number of milliseconds (not seconds!) since UNIX epoch
:param facets: if present, the top 10 most frequent values for each of the fields in
               this list will be returned along with estimated counts for each value.
               Calculating these results has a reasonable overhead so only include this
               parameter if you need it
:type facets: list or comma separated string
:param facet_limits: if present, specifies the number of top values to retrieve for the
                    facets listed within. The default number will be used if this
                    parameter is not specified or if a facet in the facets list does not
                    appear in this dict. For example, with this facet list
                    ['facet1', 'facet2', 'facet3', 'facet4'], and this facet_limits
                    dict: {'facet1': 50, 'facet4': 10}, facet1 and facet4 would be
                    limited to top 50 and 10 values respectively, whereas facet2 and
                    facet3 would be limited to the default of the top 10.
:type facet_limits: a dict
:param run_query: boolean value indicating whether the query should be run and the
                  results returned or whether the query should be created and the
                  elasticsearch query returned instead of the results. Defaults to True.
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
              parameter. This value will always be included if there were results
              otherwise None is returned. A value will also always be returned even if
              this page is the last.
:type after: a list or None

If run_query is True, then a dict with the following keys is returned instead:

:param indexes: a list of the fully qualified indexes that the query would have been run
                against
:type indexes: a list of strings
:param search: the query dict that would have been sent to elasticsearch
:type search: dict
"""

vds_basic_count = ''
vds_basic_autocomplete = ''
vds_basic_extent = ''
vds_basic_raw = ''
