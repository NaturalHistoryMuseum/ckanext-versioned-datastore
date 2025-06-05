from ckantools.decorators import auth


@auth(anon=True)
def datastore_search(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_basic_query(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_basic_count(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_basic_autocomplete(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_query_extent(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_basic_extent(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}
