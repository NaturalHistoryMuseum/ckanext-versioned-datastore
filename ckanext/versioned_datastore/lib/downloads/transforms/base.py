from abc import ABCMeta, abstractmethod


class BaseTransform(metaclass=ABCMeta):
    """
    A factory class for transforming data from a core file.
    """

    name = 'base'

    def __init__(self, **kwargs):
        self.transformer_args = kwargs

    @abstractmethod
    def __call__(self, data):
        return data
