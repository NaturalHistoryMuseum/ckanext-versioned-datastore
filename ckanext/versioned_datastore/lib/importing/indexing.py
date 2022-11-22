import copy
import logging

from ckan.plugins import PluginImplementations
from splitgill.indexing.feeders import SimpleIndexFeeder
from splitgill.indexing.indexers import Indexer
from splitgill.indexing.indexes import Index
from splitgill.indexing.utils import get_versions_and_data, DOC_TYPE

from . import stats
from .. import common
from ...interfaces import IVersionedDatastore

log = logging.getLogger(__name__)


class DatastoreIndex(Index):
    """
    Represents an index in elasticsearch for a resource in CKAN for the splitgill
    indexing process.
    """

    def __init__(
        self, config, name, version, latitude_field=None, longitude_field=None
    ):
        '''
        :param config: the splitgill config object
        :param name: the resource id, this will be used as the index name
        :param version: the version being indexed up to
        :param latitude_field: optional - the name of a field containing latitudinal data
        :param longitude_field: optional - the name of a field containing longitudinal data
        '''
        super(DatastoreIndex, self).__init__(config, name, version)
        self.latitude_field = latitude_field
        self.longitude_field = longitude_field

    def add_geo_data(self, index_doc):
        """
        Adds a geo point to the meta part of the index document. This is done in place.
        If the latitude and longitude fields have not been specified by the user, are
        not present in the data then or have invalid values then nothing happens.

        :param index_doc: the dict to be indexed
        """
        if self.latitude_field and self.longitude_field:
            # extract the latitude and longitude values
            latitude = index_doc['data'].get(self.latitude_field, None)
            longitude = index_doc['data'].get(self.longitude_field, None)
            if latitude is not None and longitude is not None:
                try:
                    # check that the values are valid
                    if -90 <= float(latitude) <= 90 and -180 <= float(longitude) <= 180:
                        # update the meta.geo key to hold the latitude longitude pair
                        index_doc['meta']['geo'] = f'{latitude},{longitude}'
                except ValueError:
                    pass

    def get_index_docs(self, mongo_doc):
        """
        Yields all the action and data dicts as a tuple for the given mongo doc. To make
        things consistent the id of the record is copied into the _id field when the
        record is indexed. This ensures that everything in the datastore index has this
        field.

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
            to_index['_id'] = int(mongo_doc['id'])

            for key, value in to_index.items():
                if isinstance(value, bool):
                    # boolean values when passed through to elasticsearch will be rejected as they
                    # currently aren't handled silently when converting to a number fails, therefore
                    # just convert it to a string here. Note that records with boolean values can
                    # only come from the API as if you upload a CSV we won't convert the value to
                    # a boolean ever
                    to_index[key] = str(value).lower()
                else:
                    # everything else can pass on through
                    to_index[key] = value

            # create the base index doc
            index_doc = self.create_index_document(to_index, version, next_version)

            # add geo data if there is any to add
            self.add_geo_data(index_doc)

            # allow other extensions implementing our interface to modify the index doc
            for plugin in PluginImplementations(IVersionedDatastore):
                index_doc = plugin.datastore_modify_index_doc(
                    self.unprefixed_name, index_doc
                )

            # yield it
            yield version, index_doc

    def get_index_create_body(self):
        """
        Returns a dict that should be used to define the index in elasticsearch. This
        should be built off of the default splitgill one unless you super know what
        you're doing.

        :return: a dict
        """
        body = super(DatastoreIndex, self).get_index_create_body()
        body.setdefault('mappings', {}).setdefault(DOC_TYPE, {}).setdefault(
            'properties', {}
        ).update(
            {
                # the id field should be an integer
                'data._id': {'type': 'long'},
            }
        )
        return body


class ResourceIndexRequest(object):
    """
    Class representing a request to index data for a resource.

    We use a class like this to avoid having a long list of arguments passed through to
    queued functions.
    """

    def __init__(self, resource, lower_version, upper_version):
        '''
        :param resource: the dict for the resource we're going to index
        :param lower_version: the lower version to index (exclusive)
        :param upper_version: the upper version to index (inclusive)
        '''
        self.resource = resource
        self.lower_version = lower_version
        self.upper_version = upper_version
        self.resource_id = self.resource['id']

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f'Index on {self.resource_id}, lower version: {self.lower_version}, '
            f'upper version: {self.upper_version}'
        )


def index_resource(request):
    """
    Indexes a resource's data from MongoDB into Elasticsearch.

    :param request: the request
    :return: True if the index took place successfully, False if not
    """
    resource_id = request.resource['id']

    log.info(
        f'Starting index of {resource_id}: {request.lower_version} > versions <= '
        f'{request.upper_version}'
    )

    # create an index feeder, this gets the records out of MongoDB and presents them for indexing
    feeder = SimpleIndexFeeder(
        common.CONFIG, resource_id, request.lower_version, request.upper_version
    )
    # create the index object which will process the documents from the feeder
    index = DatastoreIndex(
        common.CONFIG,
        resource_id,
        request.upper_version,
        latitude_field=request.resource.get('_latitude_field', None),
        longitude_field=request.resource.get('_longitude_field', None),
    )
    # create an indexer object to do the indexing
    indexer = Indexer(request.upper_version, common.CONFIG, [(feeder, index)])

    # create a stats entry so that progress can be tracked
    stats_id = stats.start_operation(
        resource_id, stats.INDEX, request.upper_version, indexer.start
    )
    # setup monitoring on the indexer so that we can update the database with stats about the
    # index operation as it progresses
    stats.monitor_indexing(stats_id, indexer)

    try:
        # actually do the index, getting back some stats
        indexing_stats = indexer.index()
    except Exception as e:
        # if there's a problem, mark it in the stats
        stats.mark_error(stats_id, e)
        log.exception(f'An error occurred during indexing of {resource_id}')
        return False

    # otherwise, we're all good, let the plugins do stuff if they want
    for plugin in PluginImplementations(IVersionedDatastore):
        try:
            plugin.datastore_after_indexing(request, indexing_stats, stats_id)
        except Exception as e:
            log.error(
                f'Error during after indexing hook handling in plugin {plugin}', e
            )
    # done, return True to indicate all went successfully
    return True
