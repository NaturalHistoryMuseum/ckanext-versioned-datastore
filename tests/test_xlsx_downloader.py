from pathlib import Path
from unittest.mock import MagicMock

from openpyxl import load_workbook

from ckanext.versioned_datastore.lib.downloads.xlsx import xlsx_writer, ALL_IN_ONE_FILE_NAME, \
    DEFAULT_SHEET_NAME, RESOURCE_ID_FIELD_NAME


def test_xlsx_single(tmpdir: Path):
    data = {'field1': 'A', 'field2': 'B', 'field3': 'C'}
    resource_id = 'resource-a'
    field_counts = {resource_id: {field: 10 for field in data}}
    request = MagicMock(separate_files=False, ignore_empty_fields=False)
    hit = MagicMock()

    with xlsx_writer(request, tmpdir, field_counts) as write:
        write(hit, data, resource_id)

    data_file = tmpdir / ALL_IN_ONE_FILE_NAME
    assert data_file.exists()

    workbook = load_workbook(filename=data_file)
    rows = list(workbook[DEFAULT_SHEET_NAME].rows)
    header = list(cell.value for cell in rows[0])
    expected_headers = [RESOURCE_ID_FIELD_NAME] + sorted(data)
    assert header == expected_headers

    for row in rows[1:]:
        row_data = dict(zip(header, [cell.value for cell in row]))
        assert row_data.pop(RESOURCE_ID_FIELD_NAME) == resource_id
        assert row_data == data
