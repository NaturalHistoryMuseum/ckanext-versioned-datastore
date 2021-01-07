import click

from ckan.plugins import toolkit
from .model.details import datastore_resource_details_table
from .model.downloads import datastore_downloads_table
from .model.slugs import datastore_slugs_table
from .model.stats import import_stats_table


def get_commands():
    return [versioned_datastore]


@click.group()
def versioned_datastore():
    '''
    Perform various tasks on the versioned datastore.
    '''
    pass


@versioned_datastore.command(name=u'initdb')
def init_db():
    '''
    Ensure the tables needed by this plugin exist.
    '''
    tables = [
        import_stats_table,
        datastore_resource_details_table,
        datastore_slugs_table,
        datastore_downloads_table,
    ]
    # create the tables if they don't exist
    for table in tables:
        if not table.exists():
            table.create()
    click.secho(u'Finished creating tables', fg=u'green')


@versioned_datastore.command()
@click.option(u'-r', u'--resource_id', u'resource_ids', multiple=True)
def reindex(resource_ids):
    '''
    Reindex either a specific resource or all resources.
    '''
    ids = set()
    context = {u'ignore_auth': True}

    if resource_ids:
        # the user has specified some resources to reindex
        ids.update(resource_ids)
    else:
        # the user hasn't specified the resources to reindex so we should get a list of all
        # resources in the system
        data_dict = {u'query': u'name:', u'offset': 0, u'limit': 100}
        while True:
            result = toolkit.get_action(u'resource_search')(context, data_dict)
            if len(result[u'results']) > 0:
                ids.update(resource[u'id'] for resource in result[u'results'])
                data_dict[u'offset'] += data_dict[u'limit']
            else:
                break

    if not ids:
        click.secho(u'No resources found to reindex', fg=u'green')
        return

    click.secho(u'{} resources to reindex'.format(len(ids)), fg=u'yellow')

    with click.progressbar(sorted(ids)) as items:
        for resource_id in items:
            try:
                result = toolkit.get_action(u'datastore_reindex')(context,
                                                                  {u'resource_id': resource_id})
                click.secho(
                    u'Queued reindex of {} as job {}'.format(resource_id, result[u'job_id']),
                    fg=u'cyan')
            except toolkit.ValidationError as e:
                click.secho(
                    u'Failed to reindex {} due to validation error: {}'.format(resource_id, e),
                    fg=u'red')

    click.secho(u'Reindexing complete', fg=u'green')
