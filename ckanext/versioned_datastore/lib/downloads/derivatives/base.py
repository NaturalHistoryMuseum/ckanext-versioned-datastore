import hashlib
import json
from abc import ABCMeta


class BaseDerivativeGenerator(metaclass=ABCMeta):
    '''
    A factory class for generating derivative files in a given format.
    '''
    name = 'base'
    extension = None

    def __init__(self, separate_files=False, ignore_empty_fields=False, **format_args):
        self.separate_files = separate_files
        self.ignore_empty_fields = ignore_empty_fields
        self.format_args = format_args

    @property
    def options(self):
        return {
            'format': self.name,
            'format_args': self.format_args,
            'separate_files': self.separate_files,
            'ignore_empty_fields': self.ignore_empty_fields
        }

    @property
    def hash(self):
        file_options_hash = hashlib.sha1(json.dumps(self.options).encode('utf-8'))
        return file_options_hash.hexdigest()
