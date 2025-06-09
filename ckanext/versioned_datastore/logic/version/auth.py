from ckantools.decorators import auth


@auth(anon=True)
def vds_version_schema(context, data_dict) -> dict:
    return {'success': True}


@auth(anon=True)
def vds_version_resource(context, data_dict) -> dict:
    return {'success': True}


@auth(anon=True)
def vds_version_record(context, data_dict) -> dict:
    return {'success': True}


@auth(anon=True)
def vds_version_round(context, data_dict) -> dict:
    return {'success': True}
