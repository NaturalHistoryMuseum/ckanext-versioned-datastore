import abc
import csv
import shutil
import zipfile
from pathlib import Path
from typing import Iterable, List, Union

import openpyxl
import xlrd
from cchardet import UniversalDetector

from ckanext.versioned_datastore.lib.common import (
    ALL_FORMATS,
    SV_FORMATS,
    XLS_FORMATS,
    XLSX_FORMATS,
    ZIP_FORMATS,
)


class ReaderNotFound(Exception):
    """
    Exception indicating that no reader could be found for the provided file format.
    """

    def __init__(self, fmt: str):
        super().__init__(f"No reader matched the format '{fmt}'")
        self.fmt = fmt


def choose_reader(resource_format: str, source: Union[Path, List[dict]]) -> 'Reader':
    """
    Chooses an appropriate reader for the provided source. If the source is a file, this
    uses the resource_format to choose the reader, if the source is a list of dicts,
    then the MemoryReader is used.

    :param resource_format: the resource format
    :param source: the source data, either a path to a file, or a list of dicts
    :returns: a Reader instance
    """
    if isinstance(source, Path):
        if resource_format in SV_FORMATS:
            return SVReader(source)
        if resource_format in XLS_FORMATS:
            return XLSReader(source)
        if resource_format in XLSX_FORMATS:
            return XLSXReader(source)
        if resource_format in ZIP_FORMATS:
            return ZipReader(source)
        raise ReaderNotFound(resource_format)
    else:
        return MemoryReader(source)


def choose_reader_for_resource(
    resource: dict, source: Union[Path, List[dict]]
) -> 'Reader':
    """
    Selects a reader for the given resource using the format primarily, and resource url
    secondarily.

    :param resource: the resource dict
    :param source: the source data, either a path to a file, or a list of dicts
    :returns: a Reader instance
    """
    # start off trying to use the format they have set/CKAN has inferred
    resource_format = resource.get('format', '')
    if not resource_format:
        # if that isn't available, try and get it from the resource url
        url = resource['url']
        if url:
            resource_format = url.rsplit('.', 1)[-1]
        # todo: could try detecting based on the file? e.g. using python-magic etc
    return choose_reader(resource_format.lower(), source)


def detect_encoding(source: Path) -> str:
    """
    Given a file, attempt to detect the character encoding it uses.

    :param source: the path to the file
    :returns: the character encoding
    """
    with source.open('rb') as f:
        detector = UniversalDetector()
        # feed the universal detector the entire file
        while True:
            chunk = f.read(8192)
            if chunk:
                detector.feed(chunk)
            else:
                detector.close()
                break

        encoding = detector.result['encoding']
        # if the detector failed to work out the encoding (unlikely) or if the
        # encoding it comes up with is ASCII, just default to UTF-8 (UTF-8 is a
        # superset of ASCII)
        if encoding is None or encoding == 'ASCII':
            encoding = 'utf-8'

    return encoding


class Reader(abc.ABC):
    """
    Abstract base class for reader implementations.
    """

    @abc.abstractmethod
    def get_name(self) -> str:
        """
        :returns: a name for the Reader instance (for logging)
        """
        ...

    @abc.abstractmethod
    def get_fields(self) -> List[str]:
        """
        Returns the list of the field names found in the source.

        :returns: the fields in the source this Reader is reading
        """
        ...

    @abc.abstractmethod
    def read(self) -> Iterable[dict]:
        """
        Actually reads the data from the source and yields dicts for each row found.

        :returns: yields dicts
        """
        ...

    @abc.abstractmethod
    def get_count(self) -> int:
        """
        Returns the number of rows in the source.

        :returns: an integer representing the number of rows
        """
        ...


class SVReader(Reader):
    """
    Class for reading csv and tsv files.
    """

    def __init__(self, source: Path):
        """
        :param source: the source file path
        """
        self.source = source
        self.encoding = detect_encoding(self.source)
        with self.source.open(encoding=self.encoding, newline='') as f:
            # instead of relying on people to correctly declare the dialect they are
            # using (from experience people are awful at this), sniff it for ourselves
            self.dialect = csv.Sniffer().sniff(f.read(1024))
        self._count = None

    def get_name(self) -> str:
        """
        Creates a name for this SVReader instance which represents the dialect and
        character encoding found (useful for debugging why someone's data wasn't
        ingested as expected).

        :returns: the name
        """
        dialect_info = ', '.join(
            f'{attr}: {str(getattr(self.dialect, attr))}'
            for attr in [
                'lineterminator',
                'quoting',
                'doublequote',
                'delimiter',
                'quotechar',
                'skipinitialspace',
            ]
        )
        dialect_info = dialect_info.encode('unicode_escape').decode('utf-8')
        return f'SV reader, encoding: {self.encoding}, dialect: [{dialect_info}]'

    def get_fields(self) -> List[str]:
        """
        Reads the first line of the source file and returns it as a list.

        :returns: the field names
        """
        with self.source.open(encoding=self.encoding, newline='') as f:
            return next(csv.reader(f, dialect=self.dialect))

    def read(self) -> Iterable[dict]:
        """
        Reads each line of the source file and yields each row as a dict.

        :returns: yields each row as a dict
        """
        with self.source.open(encoding=self.encoding, newline='') as f:
            yield from csv.DictReader(f, dialect=self.dialect)

    def get_count(self) -> int:
        if self._count is None:
            with self.source.open(encoding=self.encoding, newline='') as f:
                # count each row once, then take 1 off assuming the first row is the
                # header. Use max to avoid returning a negative value if the file is
                # just empty
                self._count = max(0, sum(1 for _ in f) - 1)
        return self._count


class XLSReader(Reader):
    """
    Reader for XLS files (i.e. old Excel spreadsheets).
    """

    def __init__(self, source: Path):
        """
        :param source: the file path to the source XLS file
        """
        self.source = source
        with self.source.open('rb') as f:
            book = xlrd.open_workbook(f.read())
            # todo: currently we don't deal with multisheeted spreadsheets, we just
            #       choose the first sheet and roll with it
            sheet = book.sheet_by_index(0)
            self.header = [str(cell.value) for cell in sheet.row(0)]
            self.rows = list(sheet.rows())[1:]

    def get_name(self) -> str:
        return 'XLS reader'

    def get_fields(self) -> List[str]:
        return self.header

    def read(self) -> Iterable[dict]:
        """
        Yields a dict for each row in the file's first sheet. We do some basic handling
        of types to ensure text, numbers, and booleans are converted correctly. Dates
        are not handled because the way dates are handled in XLS files is extremely
        complicated, and it's easier just to tell people not to do it, or use a string
        representation that Splitgill can parse.

        Empty field names, empty values, and values we can't convert are ignored.

        :returns: yields a dict per row
        """
        converters = {
            # value should be a str, just use it
            xlrd.XL_CELL_TEXT: lambda x: x,
            # value should be a float, just use it
            xlrd.XL_CELL_NUMBER: lambda x: x,
            # value should be a float and dates in xls files are crackers so just use it
            xlrd.XL_CELL_DATE: lambda x: x,
            # value should be an int, convert it to a bool
            xlrd.XL_CELL_BOOLEAN: bool,
        }
        for row in self.rows[1:]:
            yield {
                field: converters[cell.ctype](cell.value)
                for field, cell in zip(self.header, row)
                # ignore empty field names and cell types we don't want to handle
                if field and cell.ctype in converters
            }

    def get_count(self) -> int:
        return len(self.rows)


class XLSXReader(Reader):
    """
    Reader for new style Excel spreadsheets.
    """

    def __init__(self, source: Path):
        """
        :param source: the path to the XLSX file
        """
        self.source = source
        with self.source.open('rb') as f:
            workbook = openpyxl.load_workbook(f, read_only=True)
            # todo: currently we don't deal with multisheeted spreadsheets, we just
            #       choose the first sheet and roll with it
            all_rows = list(workbook.worksheets[0].rows)
            self.header = [str(cell.value) for cell in all_rows[0]]
            self.rows = all_rows[1:]

    def get_name(self) -> str:
        return 'XLSX reader'

    def get_fields(self) -> List[str]:
        return self.header

    def read(self) -> Iterable[dict]:
        """
        Yields the rows from the spreadsheet's first sheet as dicts. All type
        conversions are handled by openpyxl. Empty field names or empty cell values are
        ignored.

        :returns: a dict per row
        """
        for row in self.rows[1:]:
            yield {
                field: cell.value
                for field, cell in zip(self.header, row)
                # ignore empty field names and empty cells
                if field and cell.value is not None
            }

    def get_count(self) -> int:
        return len(self.rows)


class MemoryReader(Reader):
    """
    Reader for a list of dicts already in memory.
    """

    def __init__(self, source: List[dict]):
        """
        :param source: a list of dicts
        """
        self.source = source

    def get_name(self) -> str:
        return 'Memory reader'

    def get_fields(self) -> List[str]:
        """
        Return the fields present in the source dicts. The fields will be returned in
        the order they are found in the source from first dict to last. This ensures the
        ordering is representative of the source.

        :returns: a list of field names
        """
        fields = []
        for record_data in self.source:
            for field in record_data:
                if field not in fields:
                    fields.append(field)
        return fields

    def read(self) -> Iterable[dict]:
        yield from self.source

    def get_count(self) -> int:
        return len(self.source)


class NoCandidateFileFoundInZip(Exception):
    """
    Raised when there is no file found in the zip which we can ingest.
    """

    def __init__(self):
        super().__init__('No candidate file was found in the zip file')


class ZipReader(Reader):
    """
    A reader for zip files.

    This reader looks in the zip and inspects the files inside in alphabetical order
    until it finds a file it can read, at which point it creates a reader for the file
    and this reader is used to fulfill the abstract base class's requirements.
    """

    def __init__(self, source: Path):
        """
        :param source: the path to the zip file
        """
        with zipfile.ZipFile(source) as temp_zip:
            # sort the list of files so that we maintain a consistency when reading zips
            for name in sorted(temp_zip.namelist()):
                extension = name.rsplit('.', 1)[-1]
                if extension in ALL_FORMATS:
                    extracted_source = source.parent / 'zipped_source'
                    with extracted_source.open('wb') as s:
                        with temp_zip.open(name, 'r') as f:
                            shutil.copyfileobj(f, s)
                    try:
                        self.reader = choose_reader(extension, extracted_source)
                        break
                    except ReaderNotFound:
                        continue
            else:
                raise NoCandidateFileFoundInZip()

    def get_name(self) -> str:
        return f'Zip reader, using {self.reader.get_name()}'

    def get_fields(self) -> List[str]:
        return self.reader.get_fields()

    def read(self) -> Iterable[dict]:
        yield from self.reader.read()

    def get_count(self) -> int:
        return self.reader.get_count()
