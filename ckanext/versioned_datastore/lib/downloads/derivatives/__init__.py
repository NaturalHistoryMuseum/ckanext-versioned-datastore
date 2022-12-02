from .csv import CsvDerivativeGenerator
from .json import JsonDerivativeGenerator
from .xlsx import XlsxDerivativeGenerator
from .dwc import DwcDerivativeGenerator

derivatives = [
    CsvDerivativeGenerator,
    JsonDerivativeGenerator,
    XlsxDerivativeGenerator,
    DwcDerivativeGenerator,
]
