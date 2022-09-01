from flask import Blueprint

from ckan.plugins import toolkit
from ..model.downloads import DownloadRequest

blueprint = Blueprint(name='datastore_status', import_name=__name__, url_prefix='/status')


@blueprint.route('/download/<download_id>')
def download_status(download_id):
    dl = DownloadRequest.get(download_id)
    res_show = toolkit.get_action('resource_show')
    resources = {k: res_show({}, {'id': k}) for k in
                 dl.core_record.resource_ids_and_versions} if dl.core_record else {}

    status_friendly = {
        DownloadRequest.state_initial: toolkit._('Initialising'),
        DownloadRequest.state_core_gen: toolkit._('Searching resources'),
        DownloadRequest.state_derivative_gen: toolkit._('Generating files'),
        DownloadRequest.state_retrieving: toolkit._('Retrieving files'),
        DownloadRequest.state_complete: toolkit._('Complete'),
        DownloadRequest.state_failed: toolkit._('Failed')
    }

    return toolkit.render('status/download.html',
                          extra_vars={'download_request': dl, 'resources': resources,
                                      'status_friendly': status_friendly[dl.state]})
