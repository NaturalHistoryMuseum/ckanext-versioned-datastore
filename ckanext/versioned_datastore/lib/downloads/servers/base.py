from abc import ABCMeta, abstractmethod


class BaseFileServer(metaclass=ABCMeta):
    name = 'base'

    @abstractmethod
    def serve(self, request):
        raise NotImplemented
