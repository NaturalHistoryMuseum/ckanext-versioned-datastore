from .base import BaseFileServer
from ckan.plugins import toolkit
import os


class DirectFileServer(BaseFileServer):
    name = 'direct'

    def serve(self, request):
        filepath = request.derivative_record.filepath
        filename = os.path.split(filepath)[-1]
        return toolkit.config.get('ckan.site_url') + f'/downloads/direct/{filename}'
