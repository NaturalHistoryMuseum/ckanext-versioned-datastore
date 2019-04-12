import json

from ckan import model
from ckanext.versioned_datastore.model.details import DatastoreResourceDetails


def create_details(resource_id, version, columns):
    '''
    Helper for creating a DatastoreResourceDetails object.

    :param resource_id: the resource id of the ingested resource
    :param version: the version being ingested
    :param columns: the columns present in the ingested resource
    :return: the id of the created row
    '''
    columns_str = json.dumps(columns)
    deets = DatastoreResourceDetails(resource_id=resource_id, version=version, columns=columns_str)
    deets.add()
    deets.commit()
    return deets.id


def get_details(resource_id, version):
    '''
    Retrieve the details for a resource at a given version.

    :param resource_id: the resource id
    :param version: the version to get the details at
    :return: None or a DatastoreResourceDetails object
    '''
    return model.Session.query(DatastoreResourceDetails) \
        .filter(DatastoreResourceDetails.resource_id == resource_id) \
        .filter(DatastoreResourceDetails.version == version) \
        .first()
