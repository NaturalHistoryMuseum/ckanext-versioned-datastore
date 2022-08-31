from abc import ABCMeta, abstractmethod


class BaseNotifier(metaclass=ABCMeta):
    name = 'base'

    def __init__(self, **type_args):
        self.type_args = type_args

    @abstractmethod
    def notify(self, file_url):
        raise NotImplemented
