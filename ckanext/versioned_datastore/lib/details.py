import json
from collections import OrderedDict

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


def get_all_details(resource_id, up_to_version=None):
    '''
    Retrieve all the details for a resource and return them as an OrderedDict using the versions as
    the keys. If the up_to_version parameter is passed then any versions beyond it are ignored and
    not returned in the resulting OrderedDict.

    :param resource_id: the resource id
    :param up_to_version: the maximum version to include in the resulting OrderedDict (inclusive)
    :return: None or an OrderedDict of version: DatastoreResourceDetails objects in ascending order
    '''
    query = model.Session.query(DatastoreResourceDetails)\
        .filter(DatastoreResourceDetails.resource_id == resource_id)\
        .order_by(DatastoreResourceDetails.version.asc())

    all_details = OrderedDict()
    for details in query:
        if up_to_version is not None and details.version > up_to_version:
            break
        else:
            all_details[details.version] = details

    return all_details
