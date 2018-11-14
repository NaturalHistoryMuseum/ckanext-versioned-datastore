import copy
import logging

from ckan import plugins
from eevee.indexing.feeders import ConditionalIndexFeeder
from eevee.indexing.indexers import Indexer
from eevee.indexing.indexes import Index
from eevee.indexing.utils import get_versions_and_data, DOC_TYPE

from ckanext.versioned_datastore.interfaces import IVersionedDatastore
from ckanext.versioned_datastore.lib import stats


log = logging.getLogger(__name__)


class DatastoreIndex(Index):

    def __init__(self, config, name, version, latitude_field=None, longitude_field=None):
        super(DatastoreIndex, self).__init__(config, name, version)
        self.latitude_field = latitude_field
        self.longitude_field = longitude_field

    def add_geo_data(self, index_doc):
        '''
        Adds a geo point to the meta part of the index document. This is done in place. If the
        latitude and longitude fields have not been specified by the user or are not present then
        nothing happens.

        :param index_doc: the dict to be indexed
        '''
        if self.latitude_field and self.longitude_field:
            # extract the latitude and longitude values
            latitude = index_doc[u'data'].get(self.latitude_field, None)
            longitude = index_doc[u'data'].get(self.longitude_field, None)
            if latitude is not None and longitude is not None:
                try:
                    # check that the values are valid
                    if -90 <= float(latitude) <= 90 and -180 <= float(longitude) <= 180:
                        # update the meta.geo key to hold the latitude longitude pair
                        index_doc[u'meta'][u'geo'] = u'{},{}'.format(latitude, longitude)
                except ValueError:
                    pass

    def get_commands(self, mongo_doc):
        """
        Yields all the action and data dicts as a tuple for the given mongo doc. To make things
        consistent the id of the record is copied into the _id field when the record is indexed.
        This ensures that everything in the datastore index has this field.

        :param mongo_doc: the mongo doc to handle
        """
        # iterate over the mongo_docs versions and send them to elasticsearch
        for version, data, next_version in get_versions_and_data(mongo_doc):
            # if the data is empty, skip it, it's probably a deletion
            if not data:
                continue
            # copy the dict's data so that we can modify it safely (see get_versions_and_data doc)
            to_index = copy.deepcopy(data)
            # copy over the id of the record into the correctly named field and convert it to an
            # integer
            to_index[u'_id'] = int(mongo_doc[u'id'])

            # create the base index doc
            index_doc = self.create_index_document(to_index, version, next_version)

            # add geo data if there is any to add
            self.add_geo_data(index_doc)

            # allow other extensions implementing our interface to modify the index doc
            for plugin in plugins.PluginImplementations(IVersionedDatastore):
                index_doc = plugin.datastore_modify_index_doc(self.unprefixed_name, index_doc)

            # yield it
            yield (self.create_action(mongo_doc[u'id'], version), index_doc)

    def get_index_create_body(self):
        body = super(DatastoreIndex, self).get_index_create_body()
        body.setdefault(u'mappings', {}).setdefault(DOC_TYPE, {}).setdefault(
            u'properties', {}).update({
                # the id field should be an integer
                u'data._id': {
                    u'type': u'long'
                },
            })
        return body


def index_resource(version, config, resource):
    resource_id = resource[u'id']
    feeder = ConditionalIndexFeeder(config, resource_id)
    index = DatastoreIndex(config, resource_id, version,
                           latitude_field=resource.get(u'_latitude_field', None),
                           longitude_field=resource.get(u'_longitude_field', None))
    # then index the data
    indexer = Indexer(version, config, [(feeder, index)], monitor_update_frequency=1000)

    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(resource[u'id'], stats.INDEX, version, indexer.start)
    # register a monitor to track progress by updating the stats entry we just made
    indexer.register_monitor(stats.indexing_monitor(stats_id))

    try:
        # run the index
        index_stats = indexer.index()
        stats.finish_operation(stats_id, index_stats)
        return True
    except Exception as e:
        stats.mark_error(stats_id, e.message)
        log.exception(u'An error occurred during indexing of {}'.format(resource_id))
        return False
