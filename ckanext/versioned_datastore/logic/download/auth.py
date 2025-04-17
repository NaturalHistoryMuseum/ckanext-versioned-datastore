from ckantools.decorators import auth


@auth(anon=True)
def vds_download_queue(context, data_dict) -> dict:
    return {"success": True}


@auth(anon=True)
def vds_download_regenerate(context, data_dict) -> dict:
    return {"success": True}


@auth(anon=True)
def vds_get_avro_schema(context, data_dict) -> dict:
    return {"success": True}
