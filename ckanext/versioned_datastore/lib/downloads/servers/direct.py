from .base import BaseFileServer


class DirectFileServer(BaseFileServer):
    name = 'direct'

    def serve(self, request):
        raise NotImplemented
