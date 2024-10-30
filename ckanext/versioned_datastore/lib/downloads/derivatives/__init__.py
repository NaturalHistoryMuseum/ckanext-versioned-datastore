from .csv import CsvDerivativeGenerator
from .dwc import DwcDerivativeGenerator
from .json import JsonDerivativeGenerator
from .xlsx import XlsxDerivativeGenerator

derivatives = [
    CsvDerivativeGenerator,
    JsonDerivativeGenerator,
    XlsxDerivativeGenerator,
    DwcDerivativeGenerator,
]
