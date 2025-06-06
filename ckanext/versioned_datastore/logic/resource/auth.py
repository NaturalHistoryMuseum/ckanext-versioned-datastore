from ckantools.decorators import auth


@auth(anon=True)
def vds_resource_check(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}
