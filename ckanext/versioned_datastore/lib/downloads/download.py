from ckan.plugins import toolkit

from .loaders import get_derivative_generator, get_file_server, get_notifier
from .query import Query
from ...model.downloads import DownloadRequest
import hashlib


class DownloadRunManager:
    download_dir = toolkit.config.get('ckanext.versioned_datastore.download_dir')

    def __init__(self, query_args, derivative_args, server_args, notifier_args):
        self.query = Query.from_query_args(query_args)
        self.derivative_generator = get_derivative_generator(derivative_args.format,
                                                              separate_files=derivative_args.separate_files,
                                                              ignore_empty_fields=derivative_args.ignore_empty_fields,
                                                              **derivative_args.format_args)
        self.server = get_file_server(server_args.type, **server_args.type_args)
        self.notifier = get_notifier(notifier_args.type, **notifier_args.type_args)

        # initialises a log entry in the database
        self.request = DownloadRequest()
        self.request.save()

        # initialise attributes for completing later
        self.derivative_record = None
        self.core_record = None  # will not necessarily be used

    def run(self):
        pass

    @property
    def hash(self):
        to_hash = [
            self.query.hash,
            self.derivative_generator.hash
        ]
        download_hash = hashlib.sha1('|'.join(to_hash).encode('utf-8'))
        return download_hash.hexdigest()
