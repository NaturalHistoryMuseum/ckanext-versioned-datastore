from ckantools.decorators import auth


@auth('resource_update', {'resource_id': 'id'})
def vds_data_add(context, data_dict) -> dict:
    return {'success': True}


@auth('resource_update', {'resource_id': 'id'})
def vds_data_delete(context, data_dict) -> dict:
    return {'success': True}


@auth('resource_update', {'resource_id': 'id'})
def vds_data_sync(context, data_dict) -> dict:
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def record_show(context, data_dict) -> dict:
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def vds_data_get(context, data_dict) -> dict:
    return {'success': True}
