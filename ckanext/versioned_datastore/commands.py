from ckan import model

from ckan.lib.cli import CkanCommand
from ckanext.versioned_datastore.model.stats import import_stats_table
from ckanext.versioned_datastore.model.details import datastore_resource_details_table


class VersionedDatastoreCommands(CkanCommand):
    '''
    Perform various tasks on the versioned datastore.

    vds initdb                      - ensure all the tables required by this plugin exist

    To run:
        paster vds <command> <arguments> -c /etc/ckan/default/development.ini
    '''

    summary = __doc__.split(u'\n')[0]
    usage = __doc__

    def __init__(self, *args, **kwargs):
        super(CkanCommand, self).__init__(*args, **kwargs)
        # map of the available commands to the functions that perform them
        self.vds_commands = {
            u'initdb': self.initdb,
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
        # create the tables if they don't exist
        for table in [import_stats_table, datastore_resource_details_table]:
            if not table.exists(model.meta.engine):
                table.create(model.meta.engine)
