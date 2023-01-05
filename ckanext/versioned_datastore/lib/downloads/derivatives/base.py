import os
from abc import ABCMeta, abstractmethod


class BaseDerivativeGenerator(metaclass=ABCMeta):
    """
    A factory class for generating derivative files in a given format.
    """

    name = 'base'
    extension = None

    RESOURCE_ID_FIELD_NAME = 'Source resource ID'

    def __init__(self, output_dir, fields, query, resource_id=None, **format_args):
        self.output_dir = output_dir
        self.output_name = os.extsep.join(
            [resource_id or 'resource', self.extension or self.name]
        )

        if resource_id:
            self.all_fields = fields + [self.RESOURCE_ID_FIELD_NAME]
        else:
            self.all_fields = fields
        # split the fields by file; there should not be any keys here that are not in
        # self.file_paths (though this only has to contain keys for files with fields, e.g. not
        # manifest files or similar)
        self.fields = {'main': self.all_fields}

        # some derivatives might need access to the original query
        self._query = query

        self.resource_id = resource_id

        # holds any extra args not captured by the inherited __init__()
        self.format_args = format_args

        # a derivative may have multiple component files, but most will just have the one (the
        # output file). None of them _have_ to be the output filename.
        self.file_paths = {'main': os.path.join(self.output_dir, self.output_name)}
        # this will contain open file handles
        self.files = {}
        # indicators
        self._initialised = False
        self._opened = False
        self._validated = False

    def __enter__(self):
        for fn, fp in self.file_paths.items():
            self.files[fn] = open(fp, 'a')
        self._opened = True
        self.setup()
        if not self._initialised:
            self.initialise()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalise()
        for f in self.files.values():
            try:
                f.close()
            except AttributeError:
                pass
        self.files = {}
        self._opened = False

    @property
    def main_file(self):
        if 'main' in self.files:
            return self.files['main']
        if len(self.files.values()) == 1:
            return list(self.files.values())[0]
        else:
            return

    def initialise(self):
        """
        Runs after files have opened, before any records are processed.

        Only runs the first time the files are opened; in a multi-resource generator,
        files may be opened multiple times, but this will only be run once. Use setup()
        for things that need to be run every time.
        """
        self._initialised = True

    def setup(self):
        """
        Runs every time files are opened, before any records are processed.

        Runs before initialise().
        """
        pass

    def validate(self, record):
        """
        Runs when the first record is processed.
        """
        self._validated = True

    def write(self, record):
        if not self._validated:
            self.validate(record)
        self._write(record)

    def finalise(self):
        """
        Runs when files close.
        """
        pass

    def cleanup(self):
        """
        Runs when the generator has finished, i.e. after a single resource in separate-
        resources requests and after all resources in combined requests.
        """
        pass

    @abstractmethod
    def _write(self, record):
        raise NotImplemented
