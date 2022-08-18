from abc import ABCMeta


class BaseDerivativeGenerator(metaclass=ABCMeta):
    '''
    A factory class for generating derivative files in a given format.
    '''
    name = 'base'
    extension = None

    def __init__(self, **format_args):
        self.format_args = format_args

    def write(self, data):
        pass
