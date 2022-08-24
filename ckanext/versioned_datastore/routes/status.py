from ckan.model import Session
from ckan.plugins import toolkit
from flask import Blueprint
from ..model.downloads import DownloadRequest

blueprint = Blueprint(name='datastore_status', import_name=__name__, url_prefix='/status')


@blueprint.route('/download/<download_id>')
def download_status(download_id):
    dl = DownloadRequest.get(download_id)
    res_show = toolkit.get_action('resource_show')
    resources = {k: res_show({}, {'id': k}) for k in dl.core_record.resource_ids_and_versions}

    return toolkit.render('status/download.html',
                          extra_vars={'download_request': dl, 'resources': resources})
