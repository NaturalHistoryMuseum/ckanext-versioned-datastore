"""
Add new download tables.

Revision ID: 19a61e5b669f
Revises:
Create Date: 2023-01-06 10:27:56.739905
"""
import hashlib
import json
from datetime import datetime as dt
from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

# revision identifiers, used by Alembic.
revision = '19a61e5b669f'
down_revision = None
branch_labels = None
depends_on = None

Base = declarative_base()


def make_uuid():
    return str(uuid4())


def make_hash(content):
    if isinstance(content, list):
        hashable = '|'.join(map(str, content))
    else:
        hashable = str(content)
    return hashlib.sha1(hashable.encode('utf-8')).hexdigest()


class Core(Base):
    __tablename__ = 'vds_download_core'

    id = sa.Column(sa.UnicodeText, primary_key=True, default=make_uuid)
    query_hash = sa.Column(sa.UnicodeText, nullable=False, index=True)
    query = sa.Column(JSONB, nullable=False)
    query_version = sa.Column(sa.UnicodeText, nullable=False)
    resource_ids_and_versions = sa.Column(JSONB, nullable=False, default=dict)
    resource_hash = sa.Column(sa.UnicodeText, nullable=False)
    modified = sa.Column(sa.DateTime, nullable=False, default=dt.utcnow)
    total = sa.Column(sa.BigInteger, nullable=True)
    resource_totals = sa.Column(JSONB, nullable=False, default=dict)
    field_counts = sa.Column(JSONB, nullable=False, default=dict)


class Derivative(Base):
    __tablename__ = 'vds_download_derivative'

    id = sa.Column(sa.UnicodeText, primary_key=True, default=make_uuid)
    core_id = sa.Column(
        sa.UnicodeText,
        sa.ForeignKey('vds_download_core.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False,
    )
    download_hash = sa.Column(sa.UnicodeText, nullable=False, index=True)
    created = sa.Column(sa.DateTime, nullable=False, default=dt.utcnow)
    format = sa.Column(sa.UnicodeText, nullable=False)
    options = sa.Column(JSONB, nullable=True)
    filepath = sa.Column(sa.UnicodeText, nullable=True)

    core_record = orm.relationship('Core', backref='derivatives')


class Request(Base):
    __tablename__ = 'vds_download_request'

    id = sa.Column(sa.UnicodeText, primary_key=True, default=make_uuid)
    created = sa.Column(sa.DateTime, nullable=False, default=dt.utcnow)
    modified = sa.Column(sa.DateTime, nullable=False, default=dt.utcnow)
    state = sa.Column(sa.UnicodeText, nullable=False, default='initiated')
    message = sa.Column(sa.UnicodeText, nullable=True)
    derivative_id = sa.Column(
        sa.UnicodeText,
        sa.ForeignKey(
            'vds_download_derivative.id', onupdate='CASCADE', ondelete='CASCADE'
        ),
        nullable=True,
    )
    core_id = sa.Column(
        sa.UnicodeText,
        sa.ForeignKey('vds_download_core.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=True,
    )

    core_record = orm.relationship('Core', backref='requests')
    derivative_record = orm.relationship('Derivative', backref='requests')


class DatastoreDownload(Base):
    __tablename__ = 'versioned_datastore_downloads'

    id = sa.Column(sa.UnicodeText, primary_key=True, default=make_uuid)
    query_hash = sa.Column(sa.UnicodeText, nullable=False, index=True)
    query = sa.Column(JSONB, nullable=False)
    query_version = sa.Column(sa.UnicodeText, nullable=False)
    resource_ids_and_versions = sa.Column(JSONB, nullable=False)
    created = sa.Column(sa.DateTime, nullable=False, default=dt.utcnow)
    total = sa.Column(sa.BigInteger, nullable=True)
    resource_totals = sa.Column(JSONB, nullable=True)
    state = sa.Column(sa.UnicodeText, nullable=True)
    error = sa.Column(sa.UnicodeText, nullable=True)
    options = sa.Column(JSONB, nullable=True)


def upgrade():
    bind = op.get_bind()
    session = orm.Session(bind=bind)

    Core.__table__.create(bind)
    Derivative.__table__.create(bind)
    Request.__table__.create(bind)

    for download in session.query(DatastoreDownload):
        # calculate hashes first
        resource_hash = make_hash(sorted(download.resource_ids_and_versions.items()))
        record_hash = make_hash([download.query_hash, resource_hash])
        file_options = {  # use defaults from old datastore_queue_download def
            'format': download.options.get('format', 'csv'),
            'format_args': download.options.get('format_args', {}),
            'separate_files': download.options.get('separate_files', True),
            'ignore_empty_fields': download.options.get('ignore_empty_fields', True),
            'transform': download.options.get('transform'),
        }
        derivative_hash = make_hash(json.dumps(file_options))
        download_hash = make_hash([record_hash, derivative_hash])

        core_record = (
            session.query(Core)
            .filter(
                Core.query_hash == download.query_hash,
                Core.resource_hash == resource_hash,
            )
            .first()
        )
        if core_record:
            if (
                isinstance(download.resource_totals, dict)
                and core_record.resource_totals != download.resource_totals
            ):
                core_record.resource_totals = download.resource_totals
                core_record.modified = download.created
            if download.total is not None and core_record.total is None:
                core_record.total = download.total
                core_record.modified = download.created
        else:
            core_record = Core(
                query_hash=download.query_hash,
                query=download.query,
                query_version=download.query_version,
                resource_ids_and_versions=download.resource_ids_and_versions,
                resource_hash=resource_hash,
                modified=download.created,
                total=download.total,
                resource_totals=download.resource_totals,
            )
        session.add(core_record)
        session.commit()

        derivative_record = (
            session.query(Derivative)
            .filter(
                Derivative.download_hash == download_hash,
                Derivative.core_id == core_record.id,
            )
            .first()
        )
        if not derivative_record:
            file_format = file_options.pop('format')
            derivative_record = Derivative(
                core_id=core_record.id,
                download_hash=download_hash,
                created=download.created,
                format=file_format,
                options=file_options,
            )
            session.add(derivative_record)
            session.commit()

        new_request = Request(
            created=download.created,
            modified=download.created,
            state=download.state,
            message=download.error,
            derivative_id=derivative_record.id,
            core_id=core_record.id,
        )
        session.add(new_request)
        session.commit()

    DatastoreDownload.__table__.drop(bind)


def downgrade():
    bind = op.get_bind()
    session = orm.Session(bind=bind)

    DatastoreDownload.__table__.create(bind)

    for request in session.query(Request):
        # there are new/different states in the new model
        state = (
            'processing'
            if request.state not in ['complete', 'failed', 'zipping']
            else request.state
        )
        # broken requests may not have a linked derivative record
        if not request.derivative_id:
            options = {}
        else:
            options = request.derivative_record.options
            options['format'] = request.derivative_record.format
        # or core record
        if request.core_record:
            core_record = request.core_record
        elif request.derivative_record and request.derivative_record.core_record:
            # somehow the derivative record is linked and has a core record (this
            # shouldn't happen but you never know)
            core_record = request.derivative_record.core_record
        else:
            # this will have no info so we just have to skip it
            print(f'Skipping {request.id} (state: {request.state}) due to lack of data')
            continue

        new_download = DatastoreDownload(
            query_hash=core_record.query_hash,
            query=core_record.query,
            query_version=core_record.query_version,
            resource_ids_and_versions=core_record.resource_ids_and_versions,
            created=request.created,
            total=core_record.total,
            resource_totals=core_record.resource_totals,
            state=state,
            error=request.message if request.state == 'failed' else None,
            options=options,
        )
        session.add(new_download)
        session.commit()

    Request.__table__.drop(bind)
    Derivative.__table__.drop(bind)
    Core.__table__.drop(bind)
