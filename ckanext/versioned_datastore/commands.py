from ckan import model

from ckan.lib.cli import CkanCommand
from ckanext.versioned_datastore.model.stats import import_stats_table
from ckanext.versioned_datastore.model.details import datastore_resource_details_table


class VersionedDatastoreInitDBCommand(CkanCommand):
    """
    paster --plugin=ckanext-versioned-datastore initdb -c /etc/ckan/default/development.ini
    """

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        self._load_config()
        # create the tables if they don't exist
        for table in [import_stats_table, datastore_resource_details_table]:
            if not table.exists(model.meta.engine):
                table.create(model.meta.engine)
