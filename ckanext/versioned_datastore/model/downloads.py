from datetime import datetime

from ckan.model import meta, DomainObject, Session
from ckan.model.types import make_uuid
from sqlalchemy import Column, Table, BigInteger, UnicodeText, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, backref

# this one is outside DownloadRequest so we can use it as a default in the table def
state_initial = 'initiated'

# describes the core (Parquet) files from which the output derivatives (e.g. CSV, DwC-A) are
# generated
datastore_downloads_core_files_table = Table(
    'vds_download_core',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column('query_hash', UnicodeText, nullable=False, index=True),
    Column('query', JSONB, nullable=False),
    Column('query_version', UnicodeText, nullable=False),
    Column('resource_ids_and_versions', JSONB, nullable=False),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('total', BigInteger, nullable=True),
    Column('resource_totals', JSONB, nullable=True),
    Column('filename', UnicodeText, nullable=True)
)

# describes derived files generated from the core Parquet files
datastore_downloads_derivative_files_table = Table(
    'vds_download_derivative',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column('core_id', UnicodeText,
           ForeignKey('vds_download_core.id', onupdate='CASCADE', ondelete='CASCADE'),
           nullable=False),
    Column('download_hash', UnicodeText, nullable=False, index=True),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('format', UnicodeText, nullable=False),
    Column('options', JSONB, nullable=True),
    Column('filename', UnicodeText, nullable=True)
)

datastore_downloads_requests_table = Table(
    'vds_download_request',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('modified', DateTime, nullable=False, default=datetime.utcnow),
    Column('state', UnicodeText, nullable=False, default=state_initial),
    Column('message', UnicodeText, nullable=True),
    Column('derivative_id', UnicodeText,
           ForeignKey('vds_download_derivative.id', onupdate='CASCADE', ondelete='CASCADE'),
           nullable=True)
)


class CoreFileRecord(DomainObject):
    '''
    A record of a generated core download file. Does not track individual downloads.
    '''
    id: str
    query_hash: str
    query: dict
    query_version: str
    resource_ids_and_versions: dict
    created: datetime
    total: int
    resource_totals: dict
    filename: str
    derivatives: list
    requests: list

    @classmethod
    def get(cls, record_id):
        return Session.query(cls).get(record_id)

    def update(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self, field, value)
        self.save()

    @classmethod
    def get_by_hash(cls, query_hash):
        return Session.query(cls).filter(cls.query_hash == query_hash).one_or_none()

    @classmethod
    def get_by_filename(cls, filename):
        return Session.query(cls).filter(cls.filename == filename).one_or_none()


class DerivativeFileRecord(DomainObject):
    '''
    A record of a download file requested by a user in a specific format. Does not track individual
    downloads, just lists the options used to generate a file.
    '''
    id: str
    core_id: str
    download_hash: str
    created: datetime
    format: str
    options: dict
    filename: str
    core_record: CoreFileRecord
    requests: list

    @classmethod
    def get(cls, record_id):
        return Session.query(cls).get(record_id)

    def update(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self, field, value)
        self.save()

    @classmethod
    def get_by_hash(cls, download_hash):
        return Session.query(cls).filter(cls.download_hash == download_hash).one_or_none()

    @classmethod
    def get_by_filename(cls, filename):
        return Session.query(cls).filter(cls.filename == filename).one_or_none()


class DownloadRequest(DomainObject):
    '''
    A record of an individual download request.
    '''
    id: str
    created: datetime
    modified: datetime
    state: str
    message: str
    derivative_id: str
    derivative_record: DerivativeFileRecord
    core_record: CoreFileRecord

    state_initial = state_initial
    state_complete = 'complete'
    state_failed = 'failed'
    state_packaging = 'zipping'
    state_derivative_gen = 'gen_derivative'
    state_core_gen = 'gen_core'
    state_retrieving = 'retrieving'

    @classmethod
    def get(cls, request_id):
        return Session.query(cls).get(request_id)

    def update(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self, field, value)
        self.modified = datetime.utcnow()
        self.save()

    def update_status(self, status_text):
        self.state = status_text
        self.save()


meta.mapper(CoreFileRecord, datastore_downloads_core_files_table, properties={
    'requests': relationship(DownloadRequest,
                             secondary=datastore_downloads_derivative_files_table)
})
meta.mapper(DerivativeFileRecord, datastore_downloads_derivative_files_table, properties={
    'core_record': relationship(CoreFileRecord,
                                primaryjoin=datastore_downloads_derivative_files_table.c.core_id == CoreFileRecord.id,
                                backref=backref('derivatives', cascade='all, delete-orphan'))
})
meta.mapper(DownloadRequest, datastore_downloads_requests_table, properties={
    'derivative_record': relationship(DerivativeFileRecord,
                                      primaryjoin=datastore_downloads_requests_table.c.derivative_id == DerivativeFileRecord.id,
                                      backref=backref('requests', cascade='all, delete-orphan')),
    'core_record': relationship(CoreFileRecord,
                                secondary=datastore_downloads_derivative_files_table, uselist=False)
})
