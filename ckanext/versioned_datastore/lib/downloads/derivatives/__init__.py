from .csv import CsvDerivativeGenerator
from .json import JsonDerivativeGenerator
from .xlsx import XlsxDerivativeGenerator

derivatives = [CsvDerivativeGenerator, JsonDerivativeGenerator, XlsxDerivativeGenerator]
