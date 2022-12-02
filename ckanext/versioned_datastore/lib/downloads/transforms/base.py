from abc import ABCMeta, abstractmethod


class BaseTransform(metaclass=ABCMeta):
    """
    A factory class for transforming data from a core file.
    """

    name = 'base'

    @abstractmethod
    def __call__(self, data, **kwargs):
        return data
