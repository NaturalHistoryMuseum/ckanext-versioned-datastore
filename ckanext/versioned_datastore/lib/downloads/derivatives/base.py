import os
from abc import ABCMeta, abstractmethod


class BaseDerivativeGenerator(metaclass=ABCMeta):
    '''
    A factory class for generating derivative files in a given format.
    '''
    name = 'base'
    extension = None

    def __init__(self, output_dir, fields, resource_id=None, **format_args):
        self.output_dir = output_dir
        self.fields = fields
        self.resource_id = resource_id
        self.format_args = format_args
        self.file_paths = {'main': os.path.join(self.output_dir, os.extsep.join(
            [resource_id or 'resource', self.extension or self.name]))}
        self.files = {}
        self.initialised = False

    @abstractmethod
    def initialise(self):
        raise NotImplemented

    @abstractmethod
    def write(self, data):
        raise NotImplemented

    @property
    def main_file(self):
        if 'main' in self.files:
            return self.files['main']
        if len(self.files.values()) == 1:
            return list(self.files.values())[0]
        else:
            return

    def __enter__(self):
        for fn, fp in self.file_paths.items():
            self.files[fn] = open(fp, 'a')
        if not self.initialised:
            self.initialise()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for f in self.files.values():
            try:
                f.close()
            except AttributeError:
                pass
        self.files = {}
