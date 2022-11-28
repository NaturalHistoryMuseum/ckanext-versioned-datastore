import contextlib
from collections import defaultdict

import os
import csv

from .utils import get_fields, filter_data_fields


def flatten_dict(data, path=None, separator=' | '):
    """
    Flattens a given dictionary so that nested dicts and lists of dicts are available
    from the root of the dict. For nested dicts, the keys in the nested dict are simply
    concatenated to the key that references the dict with a dot between each, for
    example:

        {"a": {"b": 4, "c": 6}} -> {"a.b": 4, "a.c": 6}

    This works to any nesting level.

    For lists of dicts, the common keys between them are pulled up to the level above in the same
    way as the standard nested dict, but if there are multiple dicts with the same keys the values
    associated with them are concatenated together using the separator parameter. For example:

        {"a": [{"b": 5}, {"b": 19}]} -> {"a.b": "5 | 19"}

    :param data: the dict to flatten
    :param path: the path to place all found keys under, by default this is None and therefore the
                 keys in the dict are not placed under anything and are used as is. This is really
                 only here for internal recursive purposes.
    :param separator: the string to use when concatenating lists of values, whether common ones from
                      a list of dicts, or indeed just a normal list of values
    :return: the flattened dict
    """
    flat = {}
    for key, value in data.items():
        if path is not None:
            # use a dot to indicate this key is below the parent in the path
            key = f'{path}.{key}'

        if isinstance(value, dict):
            # flatten the nested dict and then update the current dict we've got on the go
            flat.update(flatten_dict(value, path=key))
        elif isinstance(value, list):
            if all(isinstance(element, dict) for element in value):
                for element in value:
                    # iterate through the list of dicts flattening each as we go and then either
                    # just adding the value to the dict we've got on the go or appending it to the
                    # string value we're using for collecting multiples
                    for subkey, subvalue in flatten_dict(element, path=key).items():
                        if subkey not in flat:
                            flat[subkey] = subvalue
                        else:
                            flat[subkey] = f'{flat[subkey]}{separator}{subvalue}'
            else:
                flat[key] = separator.join(map(str, value))
        else:
            flat[key] = value

    return flat


def create_sv_writer(field_names, dialect, target_dir, filename):
    """
    Creates a file object and a reader that can be used to write data into the given
    file name and then returns both.

    :param field_names: the field names to include
    :param dialect: the python csv module dialect to use
    :param target_dir: the directory to write the data into
    :param filename: the filename to store the data in
    :return: a 2-tuple containing the open file and the DictWriter object
    """
    path = os.path.join(target_dir, filename)
    open_file = open(path, 'w', encoding='utf-8')
    writer = csv.DictWriter(open_file, field_names, dialect=dialect)
    writer.writeheader()
    return open_file, writer


@contextlib.contextmanager
def sv_writer(request, target_dir, field_counts, dialect, extension):
    """
    Provides the functionality to write data to *sv files (for example: csv and tsv).
    This function handles opening and closing the necessary files to write data to
    either one file or a file per resource.

    This function is a context manager and when used with `with` will yield a write function. This
    function takes 3 parameters - the elasticsearch hit object, the data dict from the hit and the
    resource id the hit came from.

    :param request: the download request object
    :param target_dir: the target directory to put the data files in
    :param field_counts: a dict of resource ids -> fields -> counts used to determine which fields
                         should be included in the *sv file headers
    :param dialect: the python csv dialect name to use
    :param extension: the extension to use on the file(s)
    :return: yields a write function
    """
    # we might be opening up a file for each resource, keep track so that we can close them after
    open_files = []

    if request.separate_files:
        # writers will be created lazily and stored in this dict using the resource ids as keys
        writers = {}
    else:
        # create a single writer object and open a single file for it
        all_field_names = get_fields(field_counts, request.ignore_empty_fields)
        # TODO: this could clash
        all_field_names = ['Source resource ID'] + all_field_names
        open_file, writer = create_sv_writer(
            all_field_names, dialect, target_dir, f'data.{extension}'
        )
        open_files.append(open_file)
        # sneekily use a defaultdict to return the same writer object regardless of the resource id
        writers = defaultdict(lambda: writer)

    try:

        def write(hit, data, resource_id):
            if request.separate_files and resource_id not in writers:
                # the file needs opening and the *sv writer object needs creating
                field_names = get_fields(
                    field_counts, request.ignore_empty_fields, [resource_id]
                )
                open_file, writer = create_sv_writer(
                    field_names, dialect, target_dir, f'{resource_id}.{extension}'
                )
                open_files.append(open_file)
                writers[resource_id] = writer

            if request.ignore_empty_fields:
                data = filter_data_fields(data, field_counts[resource_id])

            # the data dict may be nested, flatten it out first
            row = flatten_dict(data)
            if not request.separate_files:
                # if the rows are being written into one file we need to indicate which resource the
                # row came from, this is how we do that
                row['Source resource ID'] = resource_id

            # write the row to the writer, for separate files, this will select the correct writer
            # object and use it, for the single writer this will always retrieve the one writer
            # we're using and use it
            writers[resource_id].writerow(row)

        yield write
    finally:
        # make sure we close all the files we opened
        for open_file in open_files:
            open_file.close()
