import csv

from .base import BaseDerivativeGenerator
from ..utils import flatten_dict


class CsvDerivativeGenerator(BaseDerivativeGenerator):
    name = 'csv'
    extension = 'csv'

    def __init__(self, output_dir, fields, resource_id=None, **format_args):
        super(CsvDerivativeGenerator, self).__init__(output_dir, fields, resource_id,
                                                     **format_args)
        self.writer = None

    def initialise(self):
        self.writer = csv.DictWriter(self.main_file, self.fields)
        self.writer.writeheader()

    def write(self, data):
        row = flatten_dict(data)
        if self.resource_id:
            row['Resource ID'] = self.resource_id
        self.writer.writerow(row)
