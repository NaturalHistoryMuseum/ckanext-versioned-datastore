import contextlib
from collections import defaultdict
from pathlib import Path
from typing import List, Dict

from openpyxl import Workbook

from .sv import flatten_dict
from .utils import filter_data_fields, get_fields

RESOURCE_ID_FIELD_NAME = 'Source resource ID'
DEFAULT_SHEET_NAME = 'Data'
ALL_IN_ONE_FILE_NAME = 'data.xlsx'


class WorkbookWithFields:
    """
    Class to keep a Workbook and the associated fields in use within it together, plus
    some convenience methods for adding and saving data.
    """

    def __init__(self, fields: List[str]):
        '''
        :param fields: a list of field names
        '''
        self.fields = fields
        # the write only workbook avoids storing the workbook in memory and thus dodges running out
        # of RAM, see: https://openpyxl.readthedocs.io/en/stable/optimized.html#write-only-mode.
        # Note that there are some restrictions as to what you can do with a write only workbook but
        # we don't run into any of them in the current implementation and we need it so that this
        # writer doesn't eat all our RAM (as it says in the doc, RAM usage can be 50x file size! :O)
        self.workbook = Workbook(write_only=True)
        self.workbook.create_sheet(DEFAULT_SHEET_NAME)
        self.workbook.active.append(fields)

    def add(self, row: Dict[str, str]):
        """
        Add a row to the workbook's active sheet.

        :param row: the row of data as a dict
        """
        self.workbook.active.append(row.get(field) for field in self.fields)

    def save(self, path: Path):
        """
        Write the workbook to disk. This can only be called once (see write only
        workbook mode) and close the workbook.

        :param path: the path to write the workbook to
        """
        self.workbook.save(path)
        self.workbook.close()


@contextlib.contextmanager
def xlsx_writer(request, target_dir, field_counts):
    """
    An XLSX (i.e. Excel speadsheet) writer.

    :param request: the download request object
    :param target_dir: the directory to save the data to
    :param field_counts: a dict of resource ids -> fields -> counts used to determine which fields
                         should be included in the xlsx file headers
    :return: yields a write function
    """
    if request.separate_files:
        # map of resource ids to workbooks
        workbooks = {}
    else:
        all_field_names = get_fields(field_counts, request.ignore_empty_fields)
        # add the special resource id field name to the start
        all_field_names.insert(0, RESOURCE_ID_FIELD_NAME)
        workbook = WorkbookWithFields(all_field_names)
        # create a defaultdict which always returns the workbook we just created for all the data
        workbooks = defaultdict(lambda: workbook)

    try:

        def write(hit, data, resource_id):
            if request.separate_files and resource_id not in workbooks:
                field_names = get_fields(
                    field_counts, request.ignore_empty_fields, [resource_id]
                )
                workbooks[resource_id] = WorkbookWithFields(field_names)

            if request.ignore_empty_fields:
                data = filter_data_fields(data, field_counts[resource_id])

            row = flatten_dict(data)

            if not request.separate_files:
                # if the data is being written into one file we need to indicate which resource the
                # data came from, this is how we do that
                row['Source resource ID'] = resource_id

            workbooks[resource_id].add(row)

        yield write
    finally:
        target_path = Path(target_dir)
        if request.separate_files:
            # save each workbook out in turn
            for res_id, workbook_with_fields in workbooks.items():
                workbook_with_fields.save(target_path / f'{res_id}.xlsx')
        else:
            # there should only be one workbook in the workbooks dict so grab it and save it
            next(iter(workbooks.values())).save(target_path / ALL_IN_ONE_FILE_NAME)
