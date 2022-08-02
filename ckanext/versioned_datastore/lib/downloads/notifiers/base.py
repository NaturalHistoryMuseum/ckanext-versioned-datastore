from abc import ABCMeta, abstractmethod


class BaseNotifier(metaclass=ABCMeta):
    name = 'base'

    @abstractmethod
    def notify(self, request):
        raise NotImplemented
