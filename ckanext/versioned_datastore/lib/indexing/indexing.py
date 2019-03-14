import copy
import logging

from ckan import plugins
from eevee.indexing.feeders import SimpleIndexFeeder
from eevee.indexing.indexers import Indexer
from eevee.indexing.indexes import Index
from eevee.indexing.utils import get_versions_and_data, DOC_TYPE

from ckanext.versioned_datastore.interfaces import IVersionedDatastore
from ckanext.versioned_datastore.lib import stats, utils

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

            for key, value in to_index.items():
                if isinstance(value, bool):
                    # boolean values when passed through to elasticsearch will be rejected as they
                    # currently aren't handled silently when converting to a number fails, therefore
                    # just convert it to a string here. Note that records with boolean values can
                    # only come from the API as if you upload a CSV we won't convert the value to
                    # a boolean ever
                    to_index[key] = unicode(value).lower()
                else:
                    # everything else can pass on through
                    to_index[key] = value

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


class ResourceIndexRequest(object):
    '''
    Class representing a request to index data for a resource. We use a class like this to avoid
    having a long list of arguments passed through to queued functions.
    '''

    def __init__(self, resource, lower_version, upper_version):
        '''
        :param resource: the dict for the resource we're going to index
        :param lower_version: the lower version to index (exclusive)
        :param upper_version: the upper version to index (inclusive)
        '''
        self.resource = resource
        self.lower_version = lower_version
        self.upper_version = upper_version

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return u'Index on {}, lower version: {}, upper version: {}'.format(self.resource[u'id'],
                                                                           self.lower_version,
                                                                           self.upper_version)


def index_resource(request):
    resource_id = request.resource[u'id']

    log.info(u'Starting index of {}: {} > versions <= {}'.format(resource_id, request.lower_version,
                                                                 request.upper_version))

    feeder = SimpleIndexFeeder(utils.CONFIG, resource_id, request.lower_version,
                               request.upper_version)
    index = DatastoreIndex(utils.CONFIG, resource_id, request.upper_version,
                           latitude_field=request.resource.get(u'_latitude_field', None),
                           longitude_field=request.resource.get(u'_longitude_field', None))
    # then index the data
    indexer = Indexer(request.upper_version, utils.CONFIG, [(feeder, index)])

    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(resource_id, stats.INDEX, request.upper_version, indexer.start)
    # setup monitoring on the indexer so that we can update the database with stats about the
    # index operation as it progresses
    stats.monitor_indexing(stats_id, indexer)

    try:
        indexing_stats = indexer.index()
    except Exception as e:
        stats.mark_error(stats_id, e)
        log.exception(u'An error occurred during indexing of {}'.format(resource_id))
        return False

    for plugin in plugins.PluginImplementations(IVersionedDatastore):
        try:
            plugin.datastore_after_indexing(request, indexing_stats, stats_id)
        except Exception as e:
            log.error(u'Error during after indexing hook handling in plugin {}'.format(plugin), e)
    return True
