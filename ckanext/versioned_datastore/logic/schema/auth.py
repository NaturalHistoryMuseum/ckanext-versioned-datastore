from ckantools.decorators import auth


@auth(anon=True)
def vds_schema_latest(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}


@auth(anon=True)
def vds_schema_validate(context, data_dict) -> dict:
    # allow access to everyone
    return {'success': True}
