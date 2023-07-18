import click

from ckan.plugins import toolkit
from .model.details import datastore_resource_details_table
from .model.downloads import (
    datastore_downloads_requests_table,
    datastore_downloads_derivative_files_table,
    datastore_downloads_core_files_table,
)
from .model.slugs import datastore_slugs_table, navigational_slugs_table
from .model.stats import import_stats_table


def get_commands():
    return [versioned_datastore]


@click.group()
def versioned_datastore():
    """
    Perform various tasks on the versioned datastore.
    """
    pass


@versioned_datastore.command(name='initdb')
def init_db():
    """
    Ensure the tables needed by this plugin exist.
    """
    tables = [
        import_stats_table,
        datastore_resource_details_table,
        datastore_slugs_table,
        navigational_slugs_table,
        datastore_downloads_core_files_table,
        datastore_downloads_derivative_files_table,
        datastore_downloads_requests_table,
    ]
    # create the tables if they don't exist
    for table in tables:
        if not table.exists():
            table.create()
    click.secho('Finished creating tables', fg='green')


@versioned_datastore.command()
@click.option('-r', '--resource_id', 'resource_ids', multiple=True)
def reindex(resource_ids):
    """
    Reindex either a specific resource or all resources.
    """
    ids = set()
    context = {'ignore_auth': True}

    if resource_ids:
        # the user has specified some resources to reindex
        ids.update(resource_ids)
    else:
        # the user hasn't specified the resources to reindex so we should get a list of all
        # resources in the system
        data_dict = {'query': 'name:', 'offset': 0, 'limit': 100}
        while True:
            result = toolkit.get_action('resource_search')(context, data_dict)
            if len(result['results']) > 0:
                ids.update(resource['id'] for resource in result['results'])
                data_dict['offset'] += data_dict['limit']
            else:
                break

    if not ids:
        click.secho('No resources found to reindex', fg='green')
        return

    click.secho(f'Found {len(ids)} resources to reindex', fg='yellow')

    for resource_id in sorted(ids):
        try:
            result = toolkit.get_action('datastore_reindex')(
                context, {'resource_id': resource_id}
            )
            click.secho(
                f'Queued reindex of {resource_id} as job {result["job_id"]}', fg='cyan'
            )
        except toolkit.ValidationError as e:
            click.secho(
                f'Failed to reindex {resource_id} due to validation error: {e}',
                fg='red',
            )

    click.secho('Reindexing complete', fg='green')
