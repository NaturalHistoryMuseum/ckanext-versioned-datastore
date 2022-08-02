from .query import Query
from .loaders import get_derivative_generator, get_file_server, get_notifier
from ...model.downloads import DownloadRequest


class DownloadRunManager:

    def __init__(self, query_args, derivative_args, server_args, notifier_args):
        self.query = Query.from_query_args(query_args)
        self.server = get_file_server(server_args.type, **server_args.type_args)
        self.notifier = get_notifier(notifier_args.type, **notifier_args.type_args)

        # save these for later
        self.derivative_args = derivative_args

        # initialises a log entry in the database
        self.request = DownloadRequest()
        self.request.save()

    def run(self):
        pass