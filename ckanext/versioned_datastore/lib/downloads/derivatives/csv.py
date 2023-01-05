import csv

from .base import BaseDerivativeGenerator
from ..utils import flatten_dict


class CsvDerivativeGenerator(BaseDerivativeGenerator):
    name = 'csv'
    extension = 'csv'

    def __init__(
        self,
        output_dir,
        fields,
        query,
        resource_id=None,
        delimiter='comma',
        **format_args
    ):
        super(CsvDerivativeGenerator, self).__init__(
            output_dir, fields, query, resource_id, delimiter='comma', **format_args
        )
        self.delimiter = {'comma': ',', 'tab': '\t'}[delimiter]
        self.writer = None

    def initialise(self):
        self.writer.writeheader()
        super(CsvDerivativeGenerator, self).initialise()

    def setup(self):
        self.writer = csv.DictWriter(
            self.main_file, self.fields['main'], delimiter=self.delimiter
        )
        super(CsvDerivativeGenerator, self).setup()

    def finalise(self):
        self.writer = None
        super(CsvDerivativeGenerator, self).finalise()

    def _write(self, record):
        row = flatten_dict(record)
        filtered_row = {}
        for field, value in row.items():
            if value is None and field not in self.fields['main']:
                continue
            elif field not in self.fields['main']:
                raise ValueError('Unexpected field.')
            else:
                filtered_row[field] = value
        if self.resource_id:
            filtered_row[self.RESOURCE_ID_FIELD_NAME] = self.resource_id
        self.writer.writerow(filtered_row)
