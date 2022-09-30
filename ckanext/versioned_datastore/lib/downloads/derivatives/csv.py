import csv

from .base import BaseDerivativeGenerator
from ..utils import flatten_dict


class CsvDerivativeGenerator(BaseDerivativeGenerator):
    name = 'csv'
    extension = 'csv'

    def __init__(self, output_dir, fields, resource_id=None, delimiter='comma', **format_args):
        super(CsvDerivativeGenerator, self).__init__(output_dir, fields, resource_id,
                                                     delimiter='comma', **format_args)
        self.delimiter = {'comma': ',', 'tab': '\t'}[delimiter]
        self.writer = None

    def initialise(self):
        self.writer = csv.DictWriter(self.main_file, self.fields, delimiter=self.delimiter)
        self.writer.writeheader()
        super(CsvDerivativeGenerator, self).initialise()

    def write(self, data):
        row = flatten_dict(data)
        filtered_row = {}
        for field, value in row.items():
            if value is None and field not in self.fields:
                continue
            elif field not in self.fields:
                raise ValueError('Unexpected field.')
            else:
                filtered_row[field] = value
        if self.resource_id:
            filtered_row[self.RESOURCE_ID_FIELD_NAME] = self.resource_id
        self.writer.writerow(filtered_row)
