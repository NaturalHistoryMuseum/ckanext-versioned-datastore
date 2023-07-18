from abc import ABCMeta, abstractmethod


class BaseFileServer(metaclass=ABCMeta):
    name = 'base'

    def __init__(self, filename=None, **type_args):
        self.filename = filename
        self.type_args = type_args

    @abstractmethod
    def serve(self, request):
        raise NotImplemented
