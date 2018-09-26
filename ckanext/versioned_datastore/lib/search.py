from elasticsearch_dsl import Search


def prefix_field(field):
    '''
    Prefixes a the given field name with "data.". All data from the resource in eevee is stored
    under the data key in the elasticsearch record so to avoid end users needing to know that all
    fields should be referenced by their non-data.-prefixed name until they are internal to the code
    and can be prefixed before being passed on to eevee.

    :param field: the field name
    :return: data.<field>
    '''
    return 'data.{}'.format(field)


def create_search(q=None, filters=None, offset=None, limit=None, fields=None, facets=None,
                  facet_limits=None, sort=None, **kwargs):
    '''
    Given the parameters, creates a new elasticsearch-dsl Search object and returns it.

    :param q: a query string which will be searched against the meta.all field or a dict of fields
              and search values. If this is a dict then the keys (field names) are always prefixed
              with "data." unless the key is an empty string in which case the field uses is
              meta.all. This allows combination searches across meta.all and data.* fields.
    :param filters: a dict of fields and values to filter the result with
    :param offset: the offset to start the search result from (for pagination)
    :param limit: the limit to stop the search result at (for pagination)
    :param fields: a list of field names to return in the result
    :param facets: a list of field names to return an aggregation of top 10 values and counts for
    :param facet_limits: a dict of fields and their customised top n limits
    :param sort: a list of fields to sort by with ordering. By default the fields are sorted
                 ascending, but by providing "desc" after the field name a descending sort will be
                 used
    :param kwargs: as a convenience we allow a kwargs parameter which we ignore, this is useful to
                   as it allows the arguments to be passed by just unpacking the data_dict
    :return: an elasticsearch-dsl Search object
    '''
    search = Search()
    # add a free text query across all fields if there is one. This searches against meta.all which
    # is a copy field created by adding the values of each data.* field
    if q is not None and q is not u'' and q is not {}:
        if isinstance(q, basestring):
            search = search.query(u'match', **{u'meta.all': q})
        else:
            for field, query in q.items():
                if field == u'':
                    field = u'meta.all'
                else:
                    field = prefix_field(field)
                search = search.query(u'match', **{field: query})
    if filters is not None:
        for field, values in filters.items():
            if not isinstance(values, list):
                values = [values]
            field = u'{}'.format(prefix_field(field))
            for value in values:
                # filter on the keyword version of the field
                search = search.filter(u'term', **{field: value})
    if offset is not None:
        search = search.extra(from_=int(offset), size=100)
    if limit is not None:
        search = search.extra(size=int(limit))
    if fields is not None:
        search = search.source(map(prefix_field, fields))
    if sort is not None:
        sorts = []
        for field_and_sort in sort:
            if not field_and_sort.endswith(' desc') and not field_and_sort.endswith(' asc'):
                field_and_sort += u' asc'
            field, direction = prefix_field(field_and_sort).rsplit(' ', 1)
            if direction == u'desc':
                sorts.append(u'-{}'.format(field))
            else:
                sorts.append(field)
        search = search.sort(*sorts)
    else:
        # by default, sort by the _id field
        search = search.sort(prefix_field('_id'))
    if facets is not None:
        facet_limits = facet_limits if facet_limits is not None else {}
        for facet in facets:
            # to produce the facet counts we use a bucket terms aggregation, note that using the
            # bucket function on the top level aggs attribute on the search object doesn't return a
            # copy of the search object like it does when adding queries etc
            search.aggs.bucket(facet, u'terms',
                               field=prefix_field(facet),
                               size=facet_limits.get(facet, 10))
    return search
