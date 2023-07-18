from ckan.plugins import toolkit
from flask import Blueprint, send_from_directory

blueprint = Blueprint(name='downloads', import_name=__name__, url_prefix='/downloads')


@blueprint.route('/direct/<zip_name>')
def direct(zip_name):
    """
    Serves up the requested zip from the download directory. This is only registered
    with flask when running in debug mode and therefore is only for testing in
    development. In production we should always serve files through the web server.

    :param zip_name: the zip name
    :return: the send file response
    """
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')
    return send_from_directory(download_dir, zip_name)


@blueprint.route('/custom/<zip_name>')
def custom(zip_name):
    """
    Basically exactly the same as the /direct/ route, but for serving the symlinks with
    custom filenames.

    :param zip_name: the zip name
    :return: the send file response
    """
    download_dir = toolkit.config.get('ckanext.versioned_datastore.custom_dir')
    return send_from_directory(download_dir, zip_name)
