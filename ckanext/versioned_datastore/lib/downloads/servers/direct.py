import os

from ckan.plugins import toolkit

from ckanext.versioned_datastore.lib.downloads.servers.base import BaseFileServer


class DirectFileServer(BaseFileServer):
    name = 'direct'

    def serve(self, request):
        site_url = toolkit.config.get('ckan.site_url')

        if self.filename:
            return site_url + f'/downloads/custom/{self.filename}.zip'

        filepath = request.derivative_record.filepath
        filename = os.path.split(filepath)[-1]
        return site_url + f'/downloads/direct/{filename}'
