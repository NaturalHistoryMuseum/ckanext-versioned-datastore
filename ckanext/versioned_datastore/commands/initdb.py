from ckan import model

from ckan.lib.cli import CkanCommand
from ckanext.versioned_datastore.model.stats import import_stats_table


class VersionedDatastoreInitDBCommand(CkanCommand):
    """
    paster --plugin=ckanext-versioned-datastore initdb -c /etc/ckan/default/development.ini
    """

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        self._load_config()
        # create the table if it doesn't exist
        if not import_stats_table.exists(model.meta.engine):
            import_stats_table.create(model.meta.engine)
