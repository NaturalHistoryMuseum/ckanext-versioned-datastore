from ckan import logic, model, plugins

from ckan.lib.cli import CkanCommand
from ckanext.versioned_datastore.model.details import datastore_resource_details_table
from ckanext.versioned_datastore.model.stats import import_stats_table


class VersionedDatastoreCommands(CkanCommand):
    '''
    Perform various tasks on the versioned datastore.

    vds initdb                      - ensure all the tables required by this plugin exist
    vds reindex [resource_id]       - reindex all (non-readonly) resources in elasticsearch, or if a
                                      resource_id is given, just that resource is reindexed
                                      (as long as it's not readonly)

    To run from the plugin directory:
        paster vds <command> <arguments> -c /etc/ckan/default/development.ini
    or from elsewhere
        paster --plugin=ckanext-versioned-datastore vds <command> <arguments> -c /etc/ckan/default/development.ini
    '''

    summary = __doc__.split(u'\n')[0]
    usage = __doc__

    def __init__(self, *args, **kwargs):
        super(CkanCommand, self).__init__(*args, **kwargs)
        # map of the available commands to the functions that perform them
        self.vds_commands = {
            u'initdb': self.initdb,
            u'reindex': self.reindex,
        }

    def command(self):
        '''
        Main entry point for all commands. This function checks if there is a command and if there
        is calls the appropriate function to perform it.
        '''
        self._load_config()

        if self.args:
            self.vds_commands.get(self.args[0], self.command_not_recognised)()
        else:
            print VersionedDatastoreCommands.usage

    def command_not_recognised(self):
        print u'Command not recognised, options: {}'.format(u', '.join(self.vds_commands))

    def initdb(self):
        '''
        Ensure the tables needed by this plugin exist.
        '''
        # create the tables if they don't exist
        for table in [import_stats_table, datastore_resource_details_table]:
            if not table.exists(model.meta.engine):
                table.create(model.meta.engine)

    def reindex(self):
        '''
        Reindex either a specific resource or all resources.
        '''
        resource_ids = set()
        context = {u'ignore_auth': True}

        if len(self.args) > 1:
            # the user has specified a single resource to reindex
            resource_ids.add(self.args[1])
        else:
            # the user hasn't specified the resource to reindex so we should get a list of all
            # resources in the system
            data_dict = {u'query': u'name:', u'offset': 0, u'limit': 100}
            while True:
                result = logic.get_action(u'resource_search')(context, data_dict)
                if len(result[u'results']) > 0:
                    resource_ids.update(resource[u'id'] for resource in result[u'results'])
                    data_dict[u'offset'] += data_dict[u'limit']
                else:
                    break

        print u'{} resources to reindex'.format(len(resource_ids))

        for resource_id in resource_ids:
            try:
                result = logic.get_action(u'datastore_reindex')(context,
                                                                {u'resource_id': resource_id})
                print u'Queued reindex of {} as job {}'.format(resource_id, result[u'job_id'])
            except plugins.toolkit.ValidationError as e:
                print u'Failed to reindex {} due to validation error: {}'.format(resource_id, e)
