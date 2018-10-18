import copy

from eevee.indexing.feeders import ConditionalIndexFeeder
from eevee.indexing.indexers import Indexer
from eevee.indexing.indexes import Index
from eevee.indexing.utils import get_versions_and_data, DOC_TYPE

from ckanext.versioned_datastore.lib import stats


class DatastoreIndex(Index):

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

            # TODO: this is where we should handle geom loading (through a plugin loop!)

            yield (self.create_action(mongo_doc[u'id'], version),
                   self.create_index_document(to_index, version, next_version))

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


def index_resource(version, config, resource_id, stats_id):
    feeder = ConditionalIndexFeeder(config, resource_id)
    index = DatastoreIndex(config, resource_id, version)
    # then index the data
    indexer = Indexer(version, config, [(feeder, index)], monitor_update_frequency=1000)
    indexer.register_monitor(stats.indexing_monitor(stats_id))
    index_stats = indexer.index()
    stats.finish_indexing(stats_id, index_stats)
