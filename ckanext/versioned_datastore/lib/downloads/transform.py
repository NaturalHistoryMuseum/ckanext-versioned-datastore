import logging

from ckan.plugins import toolkit

log = logging.getLogger(__name__)
base_url = toolkit.config.get('ckan.site_url')
object_endpoint = toolkit.config.get(
    'ckanext.versioned_datastore.record_view_endpoint', 'object.view'
)


class Transform:
    @classmethod
    def transform_data(cls, data, config):
        id_as_url = config.get('id_as_url')
        if id_as_url:
            field = id_as_url.get('field', 'id')
            data = cls.id_as_url(data, field)
        return data

    @classmethod
    def id_as_url(cls, data, field):
        """
        Reformat an ID field as a URL (probably one that links to that record). Requires
        an endpoint (config option ckanext.versioned_datastore.record_view_endpoint,
        default 'object.view') taking the ID field as the 'uuid' named argument.

        :param data: the record data to be transformed
        :param field: the name of the data field that contains the ID and that will contain the URL
        :return: the transformed data (or untransformed if there was an error).
        """
        try:
            object_id = data.get(field)
            if object_id is None or object_id == '':
                log.error(f'Failed to get uuid from field "{field}".')
                return data
            kwargs = {'uuid': object_id}
            url = toolkit.url_for(object_endpoint, **kwargs)
        except:
            log.error(f'Failed to generate URL from ID.', exc_info=True)
            return data
        data[field] = base_url + url
        return data
