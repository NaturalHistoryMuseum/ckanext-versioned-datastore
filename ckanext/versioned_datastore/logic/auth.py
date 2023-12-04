from ckan.plugins import toolkit
from ckantools.decorators import auth


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def datastore_search(context, data_dict):
    return {'success': True}


@auth('resource_update', {'resource_id': 'id'})
def datastore_create(context, data_dict):
    return {'success': True}


@auth('resource_update', {'resource_id': 'id'})
def datastore_upsert(context, data_dict):
    return {'success': True}


@auth('resource_update', {'resource_id': 'id'})
def datastore_delete(context, data_dict):
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def datastore_get_record_versions(context, data_dict):
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def datastore_get_resource_versions(context, data_dict):
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def datastore_autocomplete(context, data_dict):
    return {'success': True}


@auth('resource_update', {'resource_id': 'id'})
def datastore_reindex(context, data_dict):
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def datastore_query_extent(context, data_dict):
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def datastore_get_rounded_version(context, data_dict):
    return {'success': True}


@auth('resource_show', {'resource_id': 'id'}, anon=True)
def datastore_search_raw(context, data_dict):
    return {'success': True}


@auth(anon=True)
def datastore_ensure_privacy(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_multisearch(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_field_autocomplete(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_value_autocomplete(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_create_slug(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_resolve_slug(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_queue_download(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_regenerate_download(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_guess_fields(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_hash_query(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_is_datastore_resource(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_get_latest_query_schema_version(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def datastore_count(context, data_dict):
    # allow access to everyone
    return {'success': True}


@auth()
def datastore_edit_slug(context, data_dict):
    # only allows logged-in users
    return {'success': True}


@auth()
def datastore_custom_download_filename(context, data_dict):
    # only allow access to admins (they usually skip this check)
    user_is_sysadmin = context.get('auth_user_obj').sysadmin
    return {'success': user_is_sysadmin}


@auth(anon=True)
def datastore_multisearch_counts(context, data_dict):
    # allow access to everyone
    return {"success": True}
