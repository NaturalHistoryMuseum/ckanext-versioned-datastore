import contextlib
from collections import defaultdict

from ckan.plugins import toolkit

from . import urls
from .archive import Archive
from .schema import Schema
from ..utils import filter_data_fields, get_fields


@contextlib.contextmanager
def dwc_writer(request, target_dir, field_counts):
    """
    Provides the functionality to write data to DarwinCore archives
    (https://dwc.tdwg.org).

    This function is a context manager and when used with `with` will yield a write function. This
    function takes 3 parameters - the elasticsearch hit object, the data dict from the hit and the
    resource id the hit came from.

    :param request: the download request object
    :param target_dir: the target directory to put the data files in
    :param field_counts: a dict of resource ids -> fields -> counts used to determine which fields
                         should be included in the csv file headers
    :return: yields a write function
    """

    # load the extensions, either from ckan.ini or from request args (request args take precedence).
    # Extensions are specified by name (as in urls.py).
    schema_args = {}
    # there can only be one core extension
    core_ext = request.format_args.get(
        'core_extension_name',
        toolkit.config.get('ckanext.versioned_datastore.dwc_core_extension_name'),
    )
    if core_ext is not None and core_ext.lower() in urls.core_extensions:
        core_ext_url = urls.core_extensions.get(core_ext.lower())
        schema_args['core_extension_url'] = core_ext_url

    # there can be multiple non-core extensions, separated by commas in ckan.ini or supplied as a
    # list in the request args
    ext_names = request.format_args.get(
        'extension_names',
        [
            e.strip().lower()
            for e in toolkit.config.get(
                'ckanext.versioned_datastore.dwc_extension_names', ''
            ).split(',')
        ],
    )
    # the fields used by the extension can also be overridden by request args
    config_extension_map = request.format_args.get('extension_map', {})
    ext_urls = []
    for e in ext_names:
        ext = urls.extensions.get(e)
        if not ext:
            continue
        fields = config_extension_map.get(e)
        if fields is not None and isinstance(fields, list):
            ext.fields = fields
        if ext.fields:  # no point in adding it if no fields are defined
            ext_urls.append(ext)

    if len(ext_urls) > 0:
        schema_args['extension_urls'] = ext_urls
    schema_controller = Schema.load(
        toolkit.config.get('ckanext.versioned_datastore.dwc_schema_cache'),
        **schema_args
    )

    if request.separate_files:
        # files will be opened lazily and stored in this dict using the resource ids as keys
        open_files = {}
    else:
        all_field_names = get_fields(field_counts, request.ignore_empty_fields)
        # each 'Archive' contains multiple open files
        open_file = Archive(schema_controller, request, target_dir).open(
            all_field_names
        )
        # ensure that any request to get the open file for a given resource returns this archive
        open_files = defaultdict(lambda: open_file)

    try:

        def write(hit, data, resource_id):
            if request.separate_files and resource_id not in open_files:
                resource_field_names = get_fields(
                    field_counts, request.ignore_empty_fields, [resource_id]
                )
                archive = Archive(
                    schema_controller, request, target_dir, resource_id
                ).open(resource_field_names)
                # lazily open the archive for this resource id
                open_files[resource_id] = archive
            else:
                archive = open_files[resource_id]

            if request.ignore_empty_fields:
                data = filter_data_fields(data, field_counts[resource_id])

            archive.initialise(data)

            # always add the resource ID
            data['datasetID'] = resource_id

            # and the record type
            data['basisOfRecord'] = archive.schema.row_type_name

            # write the data
            archive.write_record(data)

        yield write
    finally:
        # make sure we close all the files we opened
        for open_file in open_files.values():
            open_file.close()
