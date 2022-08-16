import hashlib
import os
from glob import iglob

from ckan.plugins import toolkit

from .core_generator import generate_core
from .loaders import get_derivative_generator, get_file_server, get_notifier
from .query import Query
from ...model.downloads import DownloadRequest, DerivativeFileRecord, CoreFileRecord


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
        self.get_derivative()

    @property
    def hash(self):
        to_hash = [
            self.query.record_hash,
            self.derivative_generator.hash
        ]
        download_hash = hashlib.sha1('|'.join(to_hash).encode('utf-8'))
        return download_hash.hexdigest()

    def _check_for_file(self, hash_string, model_class, ext='zip'):
        '''
        Helper for searching for files via iglob.
        :param hash_string: the hash string that's part of the file name
        :param model_class: the model class (i.e. DerivativeFileRecord or CoreFileRecord)
        :param ext: the extension of the file to search for
        :return: the record if the file and record exist, None if not
        '''
        # check the download dir exists
        if not os.path.exists(self.download_dir):
            os.mkdir(self.download_dir)
            # if it doesn't then the file obviously doesn't exist either
            return False

        fn = f'*_{hash_string}.{ext}'
        existing_file = next(iglob(os.path.join(self.download_dir, fn)), None)
        record = None
        if existing_file is not None:
            record = model_class.get_by_filepath(existing_file)
        return record

    def check_for_derivative(self):
        self.derivative_record = self._check_for_file(self.hash, DerivativeFileRecord)
        return self.derivative_record is not None

    def get_derivative(self):
        '''
        Find or create a derivative file and return the associated database entry (a DerivativeFileRecord instance).
        :return:
        '''
        # does derivative exist?
        derivative_exists = self.check_for_derivative()

        if derivative_exists:
            self.request.update_status(DownloadRequest.state_retrieving)
            return self.derivative_record

        self.core_record = generate_core(self.query, self.request)

        self.request.update_status(DownloadRequest.state_derivative_gen)
        self.derivative_record = self.derivative_generator.generate(self.core_record, self.request)
        return self.derivative_record
