from ckantools.decorators import auth


@auth(anon=True)
def vds_download_queue(context, data_dict) -> dict:
    return {'success': True}


@auth(anon=True)
def vds_download_regenerate(context, data_dict) -> dict:
    return {'success': True}


@auth(anon=True)
def vds_get_avro_schema(context, data_dict) -> dict:
    return {'success': True}


@auth()
def vds_custom_download_filename(context, data_dict):
    # only allow access to admins (they usually skip this check)
    user_is_sysadmin = context.get('auth_user_obj').sysadmin
    return {'success': user_is_sysadmin}
