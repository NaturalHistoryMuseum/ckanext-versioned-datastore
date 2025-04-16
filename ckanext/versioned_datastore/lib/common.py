# if the resource has been side loaded into the datastore then this should be its URL
DATASTORE_ONLY_RESOURCE = '_datastore_only_resource'
# the formats we support for ingestion
SV_FORMATS = {'csv', 'tsv'}
XLS_FORMATS = {'xls'}
XLSX_FORMATS = {'xlsx'}
ZIP_FORMATS = {'zip'}
ALL_FORMATS = SV_FORMATS | XLS_FORMATS | XLSX_FORMATS | ZIP_FORMATS
# version used for resources which aren't in the datastore
NON_DATASTORE_VERSION = -1
