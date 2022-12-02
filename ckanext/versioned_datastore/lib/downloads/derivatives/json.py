import csv

from .base import BaseDerivativeGenerator
import json


class JsonDerivativeGenerator(BaseDerivativeGenerator):
    name = 'json'
    extension = 'json'

    def __init__(self, output_dir, fields, query, resource_id=None, **format_args):
        super(JsonDerivativeGenerator, self).__init__(
            output_dir, fields, query, resource_id, **format_args
        )
        self._first_row = True

    def initialise(self):
        self.main_file.write('[')
        self._first_row = True
        super(JsonDerivativeGenerator, self).initialise()

    def finalise(self):
        self.main_file.write('\n]')
        super(JsonDerivativeGenerator, self).finalise()

    def _write(self, record):
        if not self._first_row:
            self.main_file.write(',\n')
        else:
            self.main_file.write('\n')
            self._first_row = False

        if self.resource_id:
            record[self.RESOURCE_ID_FIELD_NAME] = self.resource_id

        json_text = json.dumps(record, indent=2)
        indented_json_text = '\n'.join([f'  {line}' for line in json_text.split('\n')])
        self.main_file.write(indented_json_text)
