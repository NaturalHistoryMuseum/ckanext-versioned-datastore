from ckanext.versioned_datastore.logic.validators import not_empty, resource_id_exists


def vds_resource_check() -> dict:
    return {
        'resource_id': [not_empty, str, resource_id_exists],
    }
