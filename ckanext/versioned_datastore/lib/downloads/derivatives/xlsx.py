from openpyxl import Workbook

from .base import BaseDerivativeGenerator
from ..utils import flatten_dict


class XlsxDerivativeGenerator(BaseDerivativeGenerator):
    name = 'xlsx'
    extension = 'xslx'

    DEFAULT_SHEET_NAME = 'Data'

    def __init__(self, output_dir, fields, resource_id=None, **format_args):
        super(XlsxDerivativeGenerator, self).__init__(output_dir, fields, resource_id,
                                                      **format_args)
        self.workbook = None

    def initialise(self):
        self.workbook = Workbook(write_only=True)
        self.workbook.create_sheet(self.DEFAULT_SHEET_NAME)
        self.workbook.active.append(self.fields)
        super(XlsxDerivativeGenerator, self).initialise()

    def finalise(self):
        self.workbook.save(self.file_paths['main'])
        self.workbook.close()

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
        self.workbook.active.append(filtered_row.get(field) for field in self.fields)
