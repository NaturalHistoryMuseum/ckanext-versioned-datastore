from ckantools.decorators import auth


@auth(anon=True)
def vds_slug_create(context, data_dict) -> dict:
    return {'success': True}


@auth(anon=True)
def vds_slug_resolve(context, data_dict) -> dict:
    return {'success': True}


@auth(anon=True)
def vds_slug_edit(context, data_dict) -> dict:
    return {'success': True}
