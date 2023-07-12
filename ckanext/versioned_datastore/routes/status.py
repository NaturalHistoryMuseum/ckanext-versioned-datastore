import os
from datetime import datetime as dt, timedelta

from flask import Blueprint, jsonify

from ckan.plugins import toolkit, plugin_loaded
from ..lib.downloads.loaders import get_file_server
from ..lib.downloads.servers import DirectFileServer
from ..lib.query.slugs import create_nav_slug
from ..logic.actions.meta.arg_objects import ServerArgs
from ..model.downloads import DownloadRequest

blueprint = Blueprint(
    name='datastore_status', import_name=__name__, url_prefix='/status'
)


def get_download_details(download_id):
    dl = DownloadRequest.get(download_id)

    if dl is None:
        return toolkit.render(
            'status/download.html',
            extra_vars={'download_request': None},
        )

    res_show = toolkit.get_action('resource_show')
    resources = {}
    if dl.core_record:
        for k in dl.core_record.resource_ids_and_versions:
            try:
                resources[k] = res_show({}, {'id': k})
            except:
                continue

    status_friendly = {
        DownloadRequest.state_initial: toolkit._('Waiting to start'),
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

    if dl.derivative_record.filepath:
        file_exists = os.path.exists(dl.derivative_record.filepath)
    else:
        file_exists = False

    urls = {}
    if file_exists and dl.state == DownloadRequest.state_complete:
        # include a vanilla direct link
        urls['direct'] = DirectFileServer().serve(dl)
        if dl.server_args is not None:
            server_args = ServerArgs(**dl.server_args)
            if server_args.custom_filename:
                urls['custom'] = DirectFileServer(
                    filename=server_args.custom_filename
                ).serve(dl)

            server = get_file_server(
                server_args.type,
                filename=server_args.custom_filename,
                **server_args.type_args,
            )
            url = server.serve(dl)
            if url != urls['direct'] and url != urls.get('custom'):
                urls[server.name] = url

    query_doi = None
    doi_url = None
    if plugin_loaded('query_dois'):
        from ckanext.query_dois.lib.doi import find_existing_doi
        from ckanext.query_dois.helpers import get_landing_page_url

        # query-dois only saves resources that return records
        non_zero_resources = {
            k: v
            for k, v in dl.core_record.resource_ids_and_versions.items()
            if dl.core_record.resource_totals.get(k, 0) > 0
        }

        query_doi = find_existing_doi(
            non_zero_resources,
            dl.core_record.query_hash,
            dl.core_record.query_version,
        )
        if query_doi:
            doi_url = get_landing_page_url(query_doi)

    try:
        nav_slug = create_nav_slug(
            {},
            dl.core_record.query,
            resource_ids_and_versions=dl.core_record.resource_ids_and_versions,
        )[1]
        search_url = toolkit.url_for(
            'search.view', slug=nav_slug.get_slug_string(), qualified=True
        )
    except toolkit.ValidationError:
        # if the resource is a non-datastore resource, this will fail, so just don't
        # return a search url
        search_url = None

    return {
        'download_request': dl,
        'file_exists': file_exists,
        'resources': resources,
        'status': dl.state,
        'status_friendly': status_friendly[dl.state],
        'total_time': total_time_elapsed,
        'since_last_update': since_last_updated,
        'urls': urls,
        'doi': query_doi,
        'doi_url': doi_url,
        'search_url': search_url,
    }


@blueprint.route('/download/<download_id>')
def download_status(download_id):
    details = get_download_details(download_id)
    return toolkit.render(
        'status/download.html',
        extra_vars=details,
    )


@blueprint.route('/download/<download_id>/json')
def download_status_json(download_id):
    details = get_download_details(download_id)

    download_request = details['download_request']
    details['created'] = download_request.created
    details['modified'] = download_request.modified
    details['message'] = download_request.message
    del details['download_request']  # not serialisable

    doi = details.get('doi')
    if doi:
        details['query'] = doi.query
        details['query_version'] = doi.query_version
        details['doi'] = doi.doi

    details['download_id'] = download_id
    details['total_time'] = details['total_time'].seconds
    details['since_last_update'] = details['since_last_update'].seconds
    return jsonify(details)
