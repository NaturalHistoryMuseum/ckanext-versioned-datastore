from .csv import CsvDerivativeGenerator
from .json import JsonDerivativeGenerator
from .dwc import DwcDerivativeGenerator

derivatives = [
    CsvDerivativeGenerator,
    JsonDerivativeGenerator,
    DwcDerivativeGenerator,
]
