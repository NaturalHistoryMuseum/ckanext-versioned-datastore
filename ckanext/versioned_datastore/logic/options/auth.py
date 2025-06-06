from ckantools.decorators import auth


@auth(anon=True)
def vds_options_get(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth('resource_update', {'resource_id': 'id'})
def vds_options_update(context, data_dict) -> dict:
    return {'success': True}
