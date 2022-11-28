import json
import os
from collections import defaultdict

import contextlib

from .utils import filter_data_fields


@contextlib.contextmanager
def jsonl_writer(request, target_dir, field_counts):
    """
    Provides the functionality to write data to jsonl files (JSON lines:
    http://jsonlines.org/). Each line in the file is a JSON serialised representation of
    a record. This function handles opening and closing the necessary files to write
    data to either one file or a file per resource.

    This function is a context manager and when used with `with` will yield a write function. This
    function takes 3 parameters - the elasticsearch hit object, the data dict from the hit and the
    resource id the hit came from.

    :param request: the download request object
    :param target_dir: the target directory to put the data files in
    :param field_counts: not used, just here to match the writer interface
    :return: yields a write function
    """
    if request.separate_files:
        # files will be opened lazily and stored in this dict using the resource ids as keys
        open_files = {}
    else:
        # open the single file we're going to write to
        open_file = open(os.path.join(target_dir, 'data.jsonl'), 'w', encoding='utf-8')
        # ensure that any request to get the open file for a given resource always returns the one
        # file we've opened
        open_files = defaultdict(lambda: open_file)

    try:

        def write(hit, data, resource_id):
            if request.separate_files and resource_id not in open_files:
                # lazily open the file for this resource id
                resource_file_name = os.path.join(target_dir, f'{resource_id}.jsonl')
                open_files[resource_id] = open(
                    resource_file_name, 'w', encoding='utf-8'
                )

            if request.ignore_empty_fields:
                data = filter_data_fields(data, field_counts[resource_id])

            if not request.separate_files:
                # if the data is being written into one file we need to indicate which resource the
                # data came from, this is how we do that
                data['Source resource ID'] = resource_id

            # dump the data ensuring it works correctly as unicode
            open_files[resource_id].write(json.dumps(data, ensure_ascii=False))
            open_files[resource_id].write('\n')

        yield write
    finally:
        # make sure we close all the files we opened
        for open_file in open_files.values():
            open_file.close()
