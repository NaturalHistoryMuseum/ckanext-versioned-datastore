from datetime import datetime as dt, timedelta

from ckan.plugins import toolkit, plugin_loaded
from flask import Blueprint

from ..lib.downloads.servers import servers
from ..model.downloads import DownloadRequest

blueprint = Blueprint(
    name='datastore_status', import_name=__name__, url_prefix='/status'
)


@blueprint.route('/download/<download_id>')
def download_status(download_id):
    dl = DownloadRequest.get(download_id)
    res_show = toolkit.get_action('resource_show')
    resources = (
        {k: res_show({}, {'id': k}) for k in dl.core_record.resource_ids_and_versions}
        if dl.core_record
        else {}
    )

    status_friendly = {
        DownloadRequest.state_initial: toolkit._('Initialising'),
        DownloadRequest.state_core_gen: toolkit._('Searching resources'),
        DownloadRequest.state_derivative_gen: toolkit._('Generating files'),
        DownloadRequest.state_retrieving: toolkit._('Retrieving files'),
        DownloadRequest.state_complete: toolkit._('Complete'),
        DownloadRequest.state_failed: toolkit._('Failed'),
        DownloadRequest.state_packaging: toolkit._('Packaging'),
    }

    time_now = dt.utcnow()
    end_time = dl.modified if dl.state == DownloadRequest.state_complete else time_now
    total_time_elapsed = timedelta(
        seconds=round((end_time - dl.created).total_seconds())
    )
    since_last_updated = timedelta(
        seconds=round((time_now - dl.modified).total_seconds())
    )

    if dl.state == DownloadRequest.state_complete:
        urls = {server.name: server().serve(dl) for server in servers}
    else:
        urls = {}

    query_doi = None
    if plugin_loaded('query_dois'):
        from ckanext.query_dois.lib.doi import find_existing_doi

        # query-dois only saves resources that return records
        non_zero_resources = {
            k: v
            for k, v in dl.core_record.resource_ids_and_versions.items()
            if dl.core_record.resource_totals[k] > 0
        }

        query_doi = find_existing_doi(
            non_zero_resources,
            dl.core_record.query_hash,
            dl.core_record.query_version,
        )

    return toolkit.render(
        'status/download.html',
        extra_vars={
            'download_request': dl,
            'resources': resources,
            'status_friendly': status_friendly[dl.state],
            'total_time': total_time_elapsed,
            'since_last_update': since_last_updated,
            'urls': urls,
            'doi': query_doi,
        },
    )
