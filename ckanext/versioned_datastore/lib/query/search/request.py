import dataclasses
from typing import Any, Dict, List, Optional

from elasticsearch_dsl import AttrDict, MultiSearch, Search
from elasticsearch_dsl.aggs import A
from elasticsearch_dsl.query import Bool
from elasticsearch_dsl.response import Response
from splitgill.indexing.fields import DocumentField
from splitgill.search import rebuild_data, version_query

from ckanext.versioned_datastore.lib.query.search.query import Query
from ckanext.versioned_datastore.lib.query.search.sort import Sort
from ckanext.versioned_datastore.lib.utils import (
    es_client,
    get_database,
    ivds_implementations,
    unprefix_index_name,
)


@dataclasses.dataclass
class SearchRequest:
    """
    Class representing a search request.
    """

    query: Query
    size: Optional[int] = 100
    offset: Optional[int] = 0
    after: Optional[Any] = None
    sorts: List[Sort] = dataclasses.field(default_factory=list)
    fields: List[str] = dataclasses.field(default_factory=list)
    aggs: Dict[str, A] = dataclasses.field(default_factory=dict)
    # any additional filters which shouldn't be represented as part of the main query
    extra_filter: Bool = dataclasses.field(default_factory=Bool)
    # setting this to True will cause no versions to be taken into account and all data
    # will be returned. As an example of where this is useful, consider aggregations
    # across all versions of a record or resource
    force_no_version: bool = False
    # if this request has been created based on an action call then it's useful to store
    # a reference to the original data dict that the action was called with. Defaults to
    # None, indicating there is no associated (or relevant) data dict available.
    data_dict: Optional[dict] = None
    ignore_auth: bool = False
    # optional additional request parameters to be included when performing the search
    req_params: dict = dataclasses.field(default_factory=dict)

    def add_param(self, param: str, value: Any):
        """
        Add a request parameter to be passed as part of this search request. The search
        request is completed using the _msearch endpoint on Elasticsearch so only
        parameters that work on that endpoint should be added.

        :param param: the param name
        :param value: the param value
        """
        self.req_params[param] = value

    def add_sort(self, field: str, ascending: bool = True):
        """
        Convenience wrapper to add a sort to the sort list on the given field with the
        given direction.

        :param field: the field to sort on
        :param ascending: whether to sort in ascending (True, default) or descending
            (False)
        """
        self.sorts.append(Sort(field, ascending))

    def add_agg(self, name, agg_type, *args, **kwargs):
        """
        Creates an aggregation and adds it to the dict of aggregations on this request.

        :param name: the name of the aggregation
        :param agg_type: the aggregation type
        :param args: the aggregation arguments
        :param kwargs: the aggregation kwarguments
        :returns:
        """
        self.aggs[name] = A(agg_type, *args, **kwargs)

    def indexes(self) -> List[str]:
        """
        A list of the indexes this request will search over. This list is created from
        the resource_ids specified in the query.

        :returns: a list of index names, this could include wildcards
        """
        databases = map(get_database, self.query.resource_ids)

        if self.force_no_version or self.query.version is not None:
            # todo: could check latest version for each and optimise to latest?
            return [database.indices.wildcard for database in databases]
        else:
            return [database.indices.latest for database in databases]

    def set_no_results(self):
        """
        Sets the request size to zero and removes all pagination values.

        This is useful for aggregation only results or counts where you don't need any
        hits.
        """
        self.size = 0
        self.offset = 0
        self.after = None

    def get_safe_size(self) -> int:
        """
        The size that is actually going to be used in the request to Elasticsearch. This
        method returns a value based on the `self.size` property but capped between 0
        and 1000, with a default value of 100 if `self.size` is set to None.

        :returns: a number between 0 and 1000
        """
        return max(0, min(100 if self.size is None else self.size, 1000))

    def to_search(self) -> Search:
        """
        Builds an Elasticsearch Search object with the query, indexes, sorts, size, and
        aggregations set.

        :returns: a new Elasticsearch Search object
        """
        search = (
            Search()
            .index(self.indexes())
            # we want to provide an accurate count, and damn the expense
            .extra(track_total_hits=True)
        )

        search = search.query(self.query.to_dsl())

        # add the version filter, if needed
        if not self.force_no_version and self.query.version is not None:
            search = search.filter(version_query(self.query.version))

        # add any extra filters if there are any
        if self.extra_filter.to_dict()['bool']:
            search = search.filter(self.extra_filter)

        size = self.get_safe_size()
        if size == 0:
            # an empty request
            search = search.extra(size=0)
        else:
            # add one to the size so that we can work out when there are no more hits
            search = search.extra(size=size + 1)

            # only offset or after should be used
            if self.offset:
                search = search.extra(from_=self.offset)
            elif self.after is not None:
                search = search.extra(search_after=self.after)

            # set the source if required
            if self.fields:
                fields = [f'{DocumentField.DATA}.{field}' for field in self.fields]
                # add the fields we need for the ResultRecord wrapper class to work
                fields.extend([DocumentField.ID, DocumentField.VERSION])
                search = search.source(fields)

            # add sorting
            if self.sorts:
                sorts = [sort.to_sort() for sort in self.sorts]
            else:
                sorts = [{DocumentField.VERSION: 'desc'}]
            # TODO: can we use _doc here? Is it safe across indexes?
            # always add the default sort to ensure search after values are unique
            sorts.extend([Sort.desc('_id').to_sort(), {'_index': 'desc'}])
            search = search.sort(*sorts)

        # add any aggregations
        for agg_name, agg in self.aggs.items():
            search.aggs[agg_name] = agg

        return search

    def run(self) -> 'SearchResponse':
        """
        Builds the search, runs it, and returns a SearchResponse object.

        :returns: a SearchResponse object
        """
        for plugin in ivds_implementations():
            plugin.vds_before_search(self)

        search = self.to_search()

        # use a multisearch to wrap the search to avoid any issues with URL length. When
        # you query a lot of indexes you can get errors because the URL contains all the
        # index names, comma separated, and it can cause a URL to be created which is
        # beyond the allowed length of Elasticsearch. Using a multisearch solves this
        # problem because the multisearch is sent to Elasticsearch as a POST request
        # with all parts of the search, including the indexes, as part of the payload
        multi_search = MultiSearch(using=es_client()).add(search)
        multi_search = multi_search.params(**self.req_params)
        result = next(iter(multi_search.execute()))
        return SearchResponse(self, result)


@dataclasses.dataclass
class ResultRecord:
    """
    A wrapper on a hit from the response.

    Unless source fields have been used in the search request, this will be wrapping a
    version of a record.
    """

    hit: AttrDict

    @property
    def id(self) -> str:
        """
        :returns: the record's ID
        """
        return self.hit[DocumentField.ID]

    @property
    def version(self) -> int:
        """
        :returns: the record's version
        """
        return self.hit[DocumentField.VERSION]

    @property
    def data(self) -> dict:
        """
        Rebuilds the record data from the hit in the raw Elasticsearch response using
        Splitgill's rebuild_data function and returns the data as a dict.

        :returns: the record data
        """
        return rebuild_data(self.hit[DocumentField.DATA].to_dict())

    @property
    def index(self) -> str:
        """
        :returns: the index this record comes from
        """
        return self.hit.meta.index

    @property
    def resource_id(self) -> str:
        """
        :returns: the resource this record comes from
        """
        return unprefix_index_name(self.index)


@dataclasses.dataclass
class SearchResponse:
    """
    Class wrapping the Elasticsearch search response object.
    """

    request: SearchRequest
    response: Response

    def __post_init__(self):
        # cache the safe size just in case the request gets fiddled with after the
        # search is completed (it's unlikely, but it would be a real pig of a bug to
        # track down)
        self._request_size = self.request.get_safe_size()

    @property
    def count(self) -> int:
        """
        :returns: the number of documents which matched the search request
        """
        return self.response.hits.total.value

    @property
    def hits(self) -> List[ResultRecord]:
        """
        :returns: returns the list of ResultRecords which were returned in the request
        """
        hits = [ResultRecord(hit) for hit in self.response.hits]
        if len(self.response.hits) > self._request_size:
            # there are more results than requested due to our addition of +1 to the
            # size during the request, so trim off the last hit as it wasn't asked for
            return hits[:-1]
        else:
            return hits

    @property
    def data(self) -> List[dict]:
        """
        :returns: a list of just the data dicts for each hit
        """
        return [hit.data for hit in self.hits]

    @property
    def aggs(self) -> dict:
        """
        :returns: the aggregation results, keyed by the names specified in the request
        """
        return self.response.aggs.to_dict()

    @property
    def next_after(self) -> Optional[list]:
        """
        Returns the after value to be used to get the next set of results. If there are
        no more results to get, None is returned.

        :returns: None or a list
        """
        if self.count == 0:
            return None

        if len(self.response.hits) > self._request_size:
            if 'sort' in self.hits[-1].hit.meta:
                return list(self.hits[-1].hit.meta['sort'])

        return None
