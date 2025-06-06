from .csv import CsvDerivativeGenerator
from .dwc import DwcDerivativeGenerator
from .json import JsonDerivativeGenerator

derivatives = [
    CsvDerivativeGenerator,
    JsonDerivativeGenerator,
    DwcDerivativeGenerator,
]
