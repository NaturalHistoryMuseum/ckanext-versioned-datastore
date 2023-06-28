from datetime import datetime

from sqlalchemy import (
    Column,
    Table,
    BigInteger,
    UnicodeText,
    DateTime,
    ForeignKey,
    desc,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, backref
from sqlalchemy.exc import InvalidRequestError

from ckan.model import meta, DomainObject, Session
from ckan.model.types import make_uuid

# this one is outside DownloadRequest so we can use it as a default in the table def
state_initial = 'initiated'

# describes the core files from which the output derivatives (e.g. CSV, DwC-A) are
# generated
datastore_downloads_core_files_table = Table(
    'vds_download_core',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column('query_hash', UnicodeText, nullable=False, index=True),
    Column('query', JSONB, nullable=False),
    Column('query_version', UnicodeText, nullable=False),
    Column('resource_ids_and_versions', JSONB, nullable=False, default=dict),
    Column('resource_hash', UnicodeText, nullable=False),
    Column('modified', DateTime, nullable=False, default=datetime.utcnow),
    Column('total', BigInteger, nullable=True),
    Column('resource_totals', JSONB, nullable=False, default=dict),
    Column('field_counts', JSONB, nullable=False, default=dict),
)

# describes derived files generated from the core files
datastore_downloads_derivative_files_table = Table(
    'vds_download_derivative',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column(
        'core_id',
        UnicodeText,
        ForeignKey('vds_download_core.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False,
    ),
    Column('download_hash', UnicodeText, nullable=False, index=True),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('format', UnicodeText, nullable=False),
    Column('options', JSONB, nullable=True),
    Column('filepath', UnicodeText, nullable=True),
)

datastore_downloads_requests_table = Table(
    'vds_download_request',
    meta.metadata,
    Column('id', UnicodeText, primary_key=True, default=make_uuid),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('modified', DateTime, nullable=False, default=datetime.utcnow),
    Column('state', UnicodeText, nullable=False, default=state_initial),
    Column('message', UnicodeText, nullable=True),
    Column('server_args', JSONB, nullable=True),
    Column(
        'derivative_id',
        UnicodeText,
        ForeignKey(
            'vds_download_derivative.id', onupdate='CASCADE', ondelete='CASCADE'
        ),
        nullable=True,
    ),
    Column(
        'core_id',
        UnicodeText,
        ForeignKey('vds_download_core.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=True,
    ),
)


class CoreFileRecord(DomainObject):
    """
    A record of a generated core download file.

    Does not track individual downloads.
    """

    id: str
    query_hash: str
    query: dict
    query_version: str
    resource_ids_and_versions: dict
    resource_hash: str
    modified: datetime
    total: int
    resource_totals: dict
    field_counts: dict
    derivatives: list
    requests: list

    @classmethod
    def get(cls, record_id):
        return Session.query(cls).get(record_id)

    def update(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self, field, value)
        self.modified = datetime.utcnow()
        try:
            self.save()
        except InvalidRequestError:
            self.commit()

    @classmethod
    def get_by_hash(cls, query_hash, resource_hash):
        return (
            Session.query(cls)
            .filter(cls.query_hash == query_hash, cls.resource_hash == resource_hash)
            .order_by(desc(cls.modified))
            .all()
        )

    @classmethod
    def find_resource(cls, query_hash, resource_id, resource_version, exclude=None):
        exclude = exclude or []
        have_resource = (
            Session.query(cls)
            .filter(
                cls.query_hash == query_hash,
                cls.resource_ids_and_versions.has_key(resource_id),
                cls.resource_totals.has_key(resource_id),
                cls.id.notin_(exclude),
            )
            .order_by(desc(cls.modified))
            .all()
        )
        have_resource_version = [
            r
            for r in have_resource
            if r.resource_ids_and_versions[resource_id] == resource_version
        ]
        if have_resource_version:
            return have_resource_version[0]
        return


class DerivativeFileRecord(DomainObject):
    """
    A record of a download file requested by a user in a specific format.

    Does not track individual downloads, just lists the options used to generate a file.
    """

    id: str
    core_id: str
    download_hash: str
    created: datetime
    format: str
    options: dict
    filepath: str
    core_record: CoreFileRecord
    requests: list

    @classmethod
    def get(cls, record_id):
        return Session.query(cls).get(record_id)

    def update(self, **kwargs):
        for field, value in kwargs.items():
            setattr(self, field, value)
        try:
            self.save()
        except InvalidRequestError:
            self.commit()

    @classmethod
    def get_by_hash(cls, download_hash):
        return (
            Session.query(cls)
            .filter(cls.download_hash == download_hash)
            .order_by(desc(cls.created))
            .all()
        )

    @classmethod
    def get_by_filepath(cls, filepath):
        return (
            Session.query(cls)
            .filter(cls.filepath == filepath)
            .order_by(desc(cls.created))
            .all()
        )


class DownloadRequest(DomainObject):
    """
    A record of an individual download request.
    """

    id: str
    created: datetime
    modified: datetime
    state: str
    message: str
    server_args: dict
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
        try:
            self.save()
        except InvalidRequestError:
            self.commit()

    def update_status(self, status_text, message=None):
        self.update(state=status_text, message=message)


meta.mapper(CoreFileRecord, datastore_downloads_core_files_table, properties={})

meta.mapper(
    DerivativeFileRecord,
    datastore_downloads_derivative_files_table,
    properties={
        'core_record': relationship(
            CoreFileRecord,
            primaryjoin=datastore_downloads_derivative_files_table.c.core_id
            == CoreFileRecord.id,
            backref=backref('derivatives', cascade='all, delete-orphan'),
            lazy='joined',
        )
    },
)

meta.mapper(
    DownloadRequest,
    datastore_downloads_requests_table,
    properties={
        'derivative_record': relationship(
            DerivativeFileRecord,
            primaryjoin=datastore_downloads_requests_table.c.derivative_id
            == DerivativeFileRecord.id,
            backref=backref('requests', cascade='all, delete-orphan'),
            lazy='joined',
        ),
        'core_record': relationship(
            CoreFileRecord,
            primaryjoin=datastore_downloads_requests_table.c.core_id
            == CoreFileRecord.id,
            backref=backref('requests', cascade='all, delete-orphan'),
            lazy='joined',
        ),
    },
)
