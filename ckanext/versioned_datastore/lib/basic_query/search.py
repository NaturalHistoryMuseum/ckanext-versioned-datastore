import operator

from ckan.plugins import PluginImplementations
from elasticsearch_dsl import Search

from .geo import add_geo_search
from ..datastore_utils import prefix_field
from ...interfaces import IVersionedDatastore


def _find_version(data_dict):
    """
    Retrieve the version from the data_dict. The version can be specified as a parameter
    in it's own right or as a special filter in the filters dict using the key
    __version__. Using the version parameter is preferred and will override any filter
    version value. The filter method is provided because of limitations in the CKAN
    recline.js framework used by the NHM on CKAN 2.3 where no additional parameters can
    be passed other than q, filters etc.

    :param data_dict: the data dict, this might be modified if the __version__ key is used (it will
                      be removed if present)
    :return: the version found as an integer, or None if no version was found
    """
    version = data_dict.get('version', None)

    # TODO: __version__ support should be removed once the frontend is capable of using the param
    # pop the __version__ to avoid including it in the normal search filters
    filter_version = data_dict.get('filters', {}).pop('__version__', None)
    # it'll probably be a list cause it's a normal filter as far as the frontend is concerned
    if isinstance(filter_version, list):
        # just use the first value
        filter_version = filter_version[0]

    # use the version parameter's value first if it exists
    if version is not None:
        return int(version)
    # otherwise fallback on __version__
    if filter_version is not None:
        return int(filter_version)
    # no version found, return None
    return None


def create_search(context, data_dict, original_data_dict):
    """
    Create the search object based on the parameters in the data_dict. This function
    will call plugins that implement the datastore_modify_data_dict and
    datastore_modify_search interface functions.

    :param context: the context dict
    :param data_dict: the data dict of parameters
    :return: a 3-tuple containing: the original data_dict that was passed into this function, the
                                   data_dict after modification by other plugins and finally the
                                   elasticsearch-dsl Search object
    """
    # allow other extensions implementing our interface to modify the data_dict
    for plugin in PluginImplementations(IVersionedDatastore):
        data_dict = plugin.datastore_modify_data_dict(context, data_dict)

    # extract the version
    version = _find_version(data_dict)

    # create an elasticsearch-dsl Search object by passing the expanded data dict
    search = build_search_object(**data_dict)

    # allow other extensions implementing our interface to modify the search object
    for plugin in PluginImplementations(IVersionedDatastore):
        search = plugin.datastore_modify_search(
            context, original_data_dict, data_dict, search
        )

    return original_data_dict, data_dict, version, search


def build_search_object(
    q=None,
    filters=None,
    after=None,
    offset=None,
    limit=None,
    fields=None,
    facets=None,
    facet_limits=None,
    sort=None,
    **kwargs
):
    """
    Given the parameters, creates a new elasticsearch-dsl Search object and returns it.

    :param q: a query string which will be searched against the meta.all field or a dict of fields
              and search values. If this is a dict then the keys (field names) are always prefixed
              with "data." unless the key is an empty string in which case the field uses is
              meta.all. This allows combination searches across meta.all and data.* fields.
    :param filters: a dict of fields and values to filter the result with. If a key is present that
                    is equal to "__geo__" then the value associated with it should be a dict which
                    will be treated as a geo query to be run against the `meta.geo` field. The value
                    should contain a "type" key which must have a corresponding value of "point",
                    "box" or "polygon" and then other keys that are dependant on the type:
                        - point:
                            - distance: the radius of the circle centred on the specified location
                                        within which records must lie to be matched. This can
                                        specified in any form that elasticsearch accepts for
                                        distances (see their doc, but values like 10km etc).
                            - point: the point to centre the radius on, specified as a lat, long
                                     pair in a list (i.e. [-20, 40.2]).
                        - box:
                            - points: the top left and bottom right points of the box, specified as
                                      a list of two lat/long pairs (i.e. [[-20, 40.2], [0.5, 100]]).
                        - polygon:
                            - points: a list of at least 3 lat/long pairs (i.e. [[-16, 44],
                                      [-13.1, 34.8], [15.99, 35], [5, 49]]).
    :param after: the search after value to start the search result from (for pagination). Cannot be
                  used in conjunction with offset. If both offset and after are provided then after
                  is used and offset is ignored.
    :param offset: the offset to start the search result from (for pagination)
    :param limit: the limit to stop the search result at (for pagination)
    :param fields: a list of field names to return in the result
    :param facets: a list of field names to return an aggregation of top 10 values and counts for
    :param facet_limits: a dict of fields and their customised top n limits
    :param sort: a list of fields to sort by with ordering. By default the fields are sorted
                 ascending, but by providing "desc" after the field name a descending sort will be
                 used. An ascending sort on _id is always added unless included in this list. This
                 is to ensure there is a unique tie-breaking field which is useful for ensuring
                 results stay the same each time they are requested and necessary to ensure the
                 correct result list responses when using the after parameter for pagination.
    :param kwargs: as a convenience we allow a kwargs parameter which we ignore, this is useful to
                   as it allows the arguments to be passed by just unpacking the data_dict
    :return: an elasticsearch-dsl Search object
    """
    search = Search()
    # add a free text query across all fields if there is one. This searches against meta.all which
    # is a copy field created by adding the values of each data.* field
    if q is not None and q != '' and q != {}:
        if isinstance(q, (str, int, float)):
            search = search.query(
                'match', **{'meta.all': {'query': q, 'operator': 'and'}}
            )
        elif isinstance(q, dict):
            for field, query in sorted(q.items(), key=operator.itemgetter(0)):
                # TODO: change this to __all__ to match __geo__?
                if field == '':
                    field = 'meta.all'
                else:
                    field = prefix_field(field)
                search = search.query(
                    'match', **{field: {'query': query, 'operator': 'and'}}
                )
    if filters is not None:
        for field, values in sorted(filters.items(), key=operator.itemgetter(0)):
            if not isinstance(values, list):
                values = [values]
            if field == '__geo__':
                # only pass through the first value
                search = add_geo_search(search, values[0])
            else:
                field = '{}'.format(prefix_field(field))
                for value in values:
                    # filter on the keyword version of the field
                    search = search.filter('term', **{field: value})

    # after and offset cannot be used together, prefer after over offset
    if after is not None:
        search = search.extra(search_after=after)
    elif offset is not None:
        search = search.extra(from_=int(offset))
    # add the limit or a default of 100 if there isn't one specified
    search = search.extra(size=int(limit) if limit is not None else 100)

    if fields is not None:
        search = search.source(list(map(prefix_field, fields)))
    if facets is not None:
        facet_limits = facet_limits if facet_limits is not None else {}
        for facet in facets:
            # to produce the facet counts we use a bucket terms aggregation, note that using the
            # bucket function on the top level aggs attribute on the search object doesn't return a
            # copy of the search object like it does when adding queries etc
            search.aggs.bucket(
                facet,
                'terms',
                field=prefix_field(facet),
                size=facet_limits.get(facet, 10),
            )

    # at least one sort is always added, on the _id column. This is necessary to ensure use of that
    # search_after is predictable (in the elasticsearch docs it recommends that a tie-breaker field
    # is present otherwise the response can include duplicates/missing records). The _id field is
    # always unique and therefore an ideal tie-breaker, so we make sure it's always in the sort
    sorts = []
    # if the caller passes in _id then we don't need to add it in again
    id_in_sort = False
    if sort is not None:
        for field_and_sort in sort:
            if not field_and_sort.endswith(' desc') and not field_and_sort.endswith(
                ' asc'
            ):
                # default the sort direction to ascending if nothing is provided
                field_and_sort += ' asc'
            field, direction = field_and_sort.rsplit(' ', 1)
            # set the id_in_sort boolean to True if we see the _id field in the caller defined sort
            id_in_sort = not id_in_sort and field == '_id'
            field = prefix_field(field)
            # if the sort direction is desc we need to add a minus sign in front of the field name,
            # otherwise we can just use the field name on its own as the default sort is asc
            sorts.append('-{}'.format(field) if direction == 'desc' else field)

    # by default, sort by the _id field
    if not id_in_sort:
        sorts.append(prefix_field('_id'))
    search = search.sort(*sorts)

    return search
