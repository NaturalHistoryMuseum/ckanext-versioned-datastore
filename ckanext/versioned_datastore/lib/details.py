import json

from ckan import model
from ckanext.versioned_datastore.model.details import DatastoreResourceDetails


def create_details(resource_id, version, columns):
    columns_str = json.dumps(columns)
    deets = DatastoreResourceDetails(resource_id=resource_id, version=version, columns=columns_str)
    deets.add()
    deets.commit()
    return deets.id


def get_details(resource_id, version):
    return model.Session.query(DatastoreResourceDetails) \
        .filter(DatastoreResourceDetails.resource_id == resource_id) \
        .filter(DatastoreResourceDetails.version == version) \
        .first()
