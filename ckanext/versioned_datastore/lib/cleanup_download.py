import logging
import os

from ckan.model import Session
from ckan.plugins import toolkit

from ckanext.versioned_datastore.model.downloads import CoreFileRecord

log = logging.getLogger(__name__)


def cleanup_download(resource_id):
    """
    Remove download files associated with a resource.
    """
    # set the directory paths
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
    custom_dir = toolkit.config.get(
        'ckanext.versioned_datastore.custom_dir', os.path.join(download_dir, 'custom')
    )
    core_records = (
        Session.query(CoreFileRecord)
        .filter(CoreFileRecord.resource_ids_and_versions.has_key(resource_id))
        .all()
    )
    # find every derivative file that contains the resource
    derivative_files = {
        os.path.abspath(derivative.filepath)
        for record in core_records
        for derivative in record.derivatives
        if derivative.filepath
        and os.path.commonpath(
            (os.path.realpath(download_dir), os.path.realpath(derivative.filepath))
        )
        == os.path.realpath(download_dir)
        and os.path.isfile(derivative.filepath)
    }

    # create core files list from records and directory
    core_files = set()
    for record in core_records:
        # imitate vds record
        core_file = os.path.join(
            download_dir,
            'core',
            record.query_hash,
            f'{resource_id}_{record.resource_ids_and_versions[resource_id]}.avro',
        )
        # if file is real add to list
        if os.path.isfile(core_file):
            core_files.add(core_file)

    # set files and file targets
    files = derivative_files | core_files
    file_targets = {os.path.realpath(f) for f in files}

    # check the custom dir is present
    if os.path.exists(custom_dir):
        # get symlinks that point to associated files
        symlinks = [
            os.path.join(custom_dir, f)
            for f in os.listdir(custom_dir)
            if os.path.islink(os.path.join(custom_dir, f))
        ]
        symlinks = [f for f in symlinks if os.path.realpath(f) in file_targets]
        # remove symlinks if any found
        if len(symlinks) > 0:
            log.info(f'resource {resource_id}: deleting {len(symlinks)} symlinks')
            for f in symlinks:
                try:
                    os.remove(f)
                except OSError as e:
                    log.error(f'Unable to remove {f}: {e}')

    # remove files if any found
    if len(files) > 0:
        log.info(f'resource {resource_id}: deleting {len(files)} files')

        for f in files:
            try:
                os.remove(f)
            except OSError as e:
                log.error(f'Unable to remove {f}: {e}')
