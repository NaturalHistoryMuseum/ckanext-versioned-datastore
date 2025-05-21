from ckan.authz import is_sysadmin
from ckantools.decorators import auth


@auth(anon=True)
def vds_multi_query(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_multi_count(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_multi_autocomplete_value(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_multi_autocomplete_field(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_multi_hash(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_multi_fields(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_multi_stats(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth()
def vds_multi_direct(context, data_dict):
    # only allow for admins
    return {'success': is_sysadmin(context.get('user'))}
